#!/usr/bin/env python3

import argparse
import asyncio
import os
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from aiohttp import web

from biim.hls.m3u8 import M3U8
from biim.mpeg2ts import ts
from biim.mpeg2ts.h264 import H264PES
from biim.mpeg2ts.packetize import packetize_pes, packetize_section
from biim.mpeg2ts.parser import PESParser, SectionParser
from biim.mpeg2ts.pat import PATSection
from biim.mpeg2ts.pmt import PMTSection
from biim.mpeg2ts.scte import (
  SegmentationDescriptor,
  SpliceInfoSection,
  SpliceInsert,
  TimeSignal,
)
from biim.util.reader import BufferingAsyncReader


class PlaylistResponse(web.Response):
  def __init__(self, *args, **kwargs) -> None:
    kwargs['headers'] = {
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'max-age=0',
      'Content-Type': 'application/x-mpegURL',
    }
    super().__init__(*args, **kwargs)


class StreamMP4Response(web.StreamResponse):
  def __init__(self, *args, **kwargs) -> None:
    kwargs['headers'] = {
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'max-age=36000',
      'Content-Type': 'video/mp4'
    }
    super().__init__(*args, **kwargs)


class InvalidMP4Response(web.Response):
  def __init__(self, *args, **kwargs) -> None:
    kwargs['headers'] = {
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'max-age=36000',
      'Content-Type': 'video/mp4'
    }
    kwargs['status'] = 400
    super().__init__(*args, **kwargs)


class InputStreamParser:
  def __init__(
    self, *, input: Any, m3u8: M3U8, sid: int, part_duration: float
  ) -> None:
    self.input = input
    self.m3u8 = m3u8
    self.sid = sid
    self.part_duration = part_duration

  async def connect(self) -> None:
    if self.input is not sys.stdin.buffer or os.name == 'nt':
      self.reader = BufferingAsyncReader(self.input, ts.PACKET_SIZE * 16)
    else:
      loop = asyncio.get_running_loop()
      self.reader = asyncio.StreamReader()
      protocol = asyncio.StreamReaderProtocol(self.reader)
      await loop.connect_read_pipe(lambda: protocol, self.input)

  async def read(self) -> bool:
    PAT_Parser = SectionParser(PATSection)
    PMT_Parser = SectionParser(PMTSection)
    SCTE35_Parser = SectionParser(SpliceInfoSection)
    H264_PES_Parser = PESParser(H264PES)

    LATEST_VIDEO_TIMESTAMP: int | None = None
    LATEST_VIDEO_MONOTONIC_TIME: float | None = None
    LATEST_VIDEO_SLEEP_DIFFERENCE: float = 0

    SCTE35_OUT_QUEUE: deque[tuple[str, datetime, datetime | None, dict]] = deque()
    SCTE35_IN_QUEUE: deque[tuple[str, datetime]] = deque()

    PCR_PID: int | None = None
    LATEST_PCR_VALUE: int | None = None
    LATEST_PCR_TIMESTAMP_90KHZ: int | None = 0
    LATEST_PCR_DATETIME: datetime | None = None

    PMT_PID: int | None = None
    H264_PID: int | None = None
    SCTE35_PID: int | None = None
    FIRST_IDR_DETECTED = False

    LAST_PAT: PATSection | None = None
    LAST_PMT: PMTSection | None = None
    PAT_CC = 0
    PMT_CC = 0
    H264_CC = 0

    PARTIAL_BEGIN_TIMESTAMP: int | None = None

    def push_PAT_PMT(PAT, PMT):
      nonlocal PAT_CC, PMT_CC
      nonlocal PMT_PID

      if PAT:
        packets = packetize_section(PAT, False, False, 0x00, 0, PAT_CC)
        PAT_CC = (PAT_CC + len(packets)) & 0x0F
        for p in packets:
          self.m3u8.push(p)
      if PMT:
        packets = packetize_section(PMT, False, False, cast(int, PMT_PID), 0, PMT_CC)
        PMT_CC = (PMT_CC + len(packets)) & 0x0F
        for p in packets:
          self.m3u8.push(p)

    while True:
      isEOF = False
      while True:
        sync_byte = await self.reader.read(1)
        if sync_byte == ts.SYNC_BYTE:
          break
        elif sync_byte == b'':
          isEOF = True
          break
      if isEOF:
        break

      packet = None
      try:
        packet = ts.SYNC_BYTE + await self.reader.readexactly(ts.PACKET_SIZE - 1)
      except asyncio.IncompleteReadError:
        break

      PID = ts.pid(packet)
      if PID == 0x00:
        PAT_Parser.push(packet)
        for PAT in PAT_Parser:
          if PAT.CRC32() != 0:
            continue
          LAST_PAT = PAT

          for program_number, program_map_PID in PAT:
            if program_number == 0:
              continue
            if program_number == self.sid:
              PMT_PID = program_map_PID
            elif not PMT_PID and not self.sid:
              PMT_PID = program_map_PID

          if FIRST_IDR_DETECTED:
            packets = packetize_section(PAT, False, False, 0x00, 0, PAT_CC)
            PAT_CC = (PAT_CC + len(packets)) & 0x0F
            for p in packets:
              self.m3u8.push(p)

      elif PID == PMT_PID:
        PMT_Parser.push(packet)
        for PMT in PMT_Parser:
          if PMT.CRC32() != 0:
            continue
          LAST_PMT = PMT
          PCR_PID = PMT.PCR_PID

          for stream_type, elementary_PID, _ in PMT:
            if stream_type == 0x1b:
              H264_PID = elementary_PID
            elif stream_type == 0x86:
              SCTE35_PID = elementary_PID

          if FIRST_IDR_DETECTED:
            packets = packetize_section(PMT, False, False, cast(int, PMT_PID), 0, PMT_CC)
            PMT_CC = (PMT_CC + len(packets)) & 0x0F
            for p in packets:
              self.m3u8.push(p)

      elif PID == H264_PID:
        H264_PES_Parser.push(packet)
        for H264 in H264_PES_Parser:
          if LATEST_PCR_VALUE is None:
            continue
          if LATEST_PCR_DATETIME is None:
            continue
          has_IDR = False
          timestamp = H264.dts() if H264.has_dts() else H264.pts()
          if timestamp is None:
            continue

          program_date_time: datetime = LATEST_PCR_DATETIME + timedelta(
            seconds=(((timestamp - LATEST_PCR_VALUE + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ)
          )

          for ebsp in H264:
            nal_unit_type = ebsp[0] & 0x1f
            if nal_unit_type == 5:
              has_IDR = True
              break

          if has_IDR:
            while SCTE35_OUT_QUEUE:
              if SCTE35_OUT_QUEUE[0][1] <= program_date_time:
                id, start_date, end_date, attributes = SCTE35_OUT_QUEUE.popleft()
                self.m3u8.open(id, program_date_time, end_date, **attributes)
              else:
                break
            while SCTE35_IN_QUEUE:
              if SCTE35_IN_QUEUE[0][1] <= program_date_time:
                id, end_date = SCTE35_IN_QUEUE.popleft()
                self.m3u8.close(id, program_date_time)
              else:
                break

          if has_IDR:
            if not FIRST_IDR_DETECTED:
              if LAST_PAT and LAST_PMT:
                FIRST_IDR_DETECTED = True
              if FIRST_IDR_DETECTED:
                PARTIAL_BEGIN_TIMESTAMP = timestamp
                self.m3u8.add_segment(
                  PARTIAL_BEGIN_TIMESTAMP, True, program_date_time
                )
                push_PAT_PMT(LAST_PAT, LAST_PMT)
            else:
              PART_DIFF = timestamp - cast(int, PARTIAL_BEGIN_TIMESTAMP)
              if self.part_duration * ts.HZ < PART_DIFF:
                PARTIAL_BEGIN_TIMESTAMP = int(timestamp - max(0, PART_DIFF - self.part_duration * ts.HZ))
                self.m3u8.continuous_partial(
                  PARTIAL_BEGIN_TIMESTAMP, False
                )
              PARTIAL_BEGIN_TIMESTAMP = timestamp
              self.m3u8.continuous_segment(
                PARTIAL_BEGIN_TIMESTAMP, True, program_date_time
              )
              push_PAT_PMT(LAST_PAT, LAST_PMT)
          elif PARTIAL_BEGIN_TIMESTAMP is not None:
            PART_DIFF = (timestamp - PARTIAL_BEGIN_TIMESTAMP + ts.PCR_CYCLE) % ts.PCR_CYCLE
            if self.part_duration * ts.HZ <= PART_DIFF:
              PARTIAL_BEGIN_TIMESTAMP = int(timestamp - max(0, PART_DIFF - (self.part_duration * ts.HZ)) + ts.PCR_CYCLE) % ts.PCR_CYCLE
              self.m3u8.continuous_partial(
                cast(int, PARTIAL_BEGIN_TIMESTAMP)
              )

          if FIRST_IDR_DETECTED:
            packets = packetize_pes(H264, False, False, cast(int, H264_PID), 0, H264_CC)
            H264_CC = (H264_CC + len(packets)) & 0x0F
            for p in packets:
              self.m3u8.push(p)

          if LATEST_VIDEO_TIMESTAMP is not None and LATEST_VIDEO_MONOTONIC_TIME is not None:
            TIMESTAMP_DIFF = ((timestamp - LATEST_VIDEO_TIMESTAMP + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ
            TIME_DIFF = time.monotonic() - LATEST_VIDEO_MONOTONIC_TIME
            if self.input is not sys.stdin.buffer:
              SLEEP_BEGIN = time.monotonic()
              await asyncio.sleep(max(0, TIMESTAMP_DIFF - (TIME_DIFF + LATEST_VIDEO_SLEEP_DIFFERENCE)))
              SLEEP_END = time.monotonic()
              LATEST_VIDEO_SLEEP_DIFFERENCE = (SLEEP_END - SLEEP_BEGIN) - max(0, TIMESTAMP_DIFF - (TIME_DIFF + LATEST_VIDEO_SLEEP_DIFFERENCE))
          LATEST_VIDEO_TIMESTAMP = timestamp
          LATEST_VIDEO_MONOTONIC_TIME = time.monotonic()

      elif PID == SCTE35_PID:
        self.m3u8.push(packet)
        SCTE35_Parser.push(packet)
        for SCTE35 in SCTE35_Parser:
          if SCTE35.CRC32() != 0:
            continue

          if SCTE35.splice_command_type == SpliceInfoSection.SPLICE_INSERT:
            splice_insert: SpliceInsert = cast(SpliceInsert, SCTE35.splice_command)
            id = str(splice_insert.splice_event_id)
            if splice_insert.splice_event_cancel_indicator: continue
            if not splice_insert.program_splice_flag: continue
            if splice_insert.out_of_network_indicator:
              attributes = { 'SCTE35-OUT': '0x' + ''.join([f'{b:02X}' for b in SCTE35[:]]) }
              if splice_insert.splice_immediate_flag or not splice_insert.splice_time.time_specified_flag:
                if LATEST_PCR_DATETIME is None:
                  continue
                start_date = LATEST_PCR_DATETIME

                if splice_insert.duration_flag:
                  attributes['PLANNED-DURATION'] = str(splice_insert.break_duration.duration / ts.HZ)
                  if splice_insert.break_duration.auto_return:
                    SCTE35_IN_QUEUE.append((id, start_date + timedelta(seconds=(splice_insert.break_duration.duration / ts.HZ))))
                SCTE35_OUT_QUEUE.append((id, start_date, None, attributes))
              else:
                if LATEST_PCR_VALUE is None:
                  continue
                if LATEST_PCR_DATETIME is None:
                  continue
                start_date = timedelta(seconds=(((cast(int, splice_insert.splice_time.pts_time) + SCTE35.pts_adjustment - LATEST_PCR_VALUE + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ)) + LATEST_PCR_DATETIME

                if splice_insert.duration_flag:
                  attributes['PLANNED-DURATION'] = str(splice_insert.break_duration.duration / ts.HZ)
                  if splice_insert.break_duration.auto_return:
                    SCTE35_IN_QUEUE.append((id, start_date + timedelta(seconds=(splice_insert.break_duration.duration / ts.HZ))))
                SCTE35_OUT_QUEUE.append((id, start_date, None, attributes))
            else:
              if splice_insert.splice_immediate_flag or not splice_insert.splice_time.time_specified_flag:
                if LATEST_PCR_DATETIME is None:
                  continue
                end_date = LATEST_PCR_DATETIME
                SCTE35_IN_QUEUE.append((id, end_date))
              else:
                if LATEST_PCR_VALUE is None:
                  continue
                if LATEST_PCR_DATETIME is None:
                  continue
                end_date = timedelta(seconds=(((cast(int, splice_insert.splice_time.pts_time) + SCTE35.pts_adjustment - LATEST_PCR_VALUE + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ)) + LATEST_PCR_DATETIME
                SCTE35_IN_QUEUE.append((id, end_date))

          elif SCTE35.splice_command_type == SpliceInfoSection.TIME_SIGNAL:
            time_signal: TimeSignal = cast(TimeSignal, SCTE35.splice_command)
            if LATEST_PCR_VALUE is None:
              continue
            if LATEST_PCR_DATETIME is None:
              continue
            specified_time = LATEST_PCR_DATETIME
            if time_signal.splice_time.time_specified_flag:
              specified_time = timedelta(seconds=(((cast(int, time_signal.splice_time.pts_time) + SCTE35.pts_adjustment - LATEST_PCR_VALUE + ts.PCR_CYCLE) % ts.PCR_CYCLE) / ts.HZ)) + LATEST_PCR_DATETIME
            for descriptor in SCTE35.descriptors:
              if descriptor.descriptor_tag != 0x02:
                continue
              segmentation_descriptor: SegmentationDescriptor = cast(SegmentationDescriptor, descriptor)
              id = str(segmentation_descriptor.segmentation_event_id)
              if segmentation_descriptor.segmentation_event_cancel_indicator:
                continue
              if not segmentation_descriptor.program_segmentation_flag:
                continue

              if segmentation_descriptor.segmentation_event_id in SegmentationDescriptor.ADVERTISEMENT_BEGIN:
                attributes = {
                  'SCTE35-OUT': '0x' + ''.join([f'{b:02X}' for b in SCTE35[:]])
                }
                if segmentation_descriptor.segmentation_duration_flag:
                  attributes['PLANNED-DURATION'] = str(
                    segmentation_descriptor.segmentation_duration / ts.HZ
                  )
                SCTE35_OUT_QUEUE.append((id, specified_time, None, attributes))
              elif segmentation_descriptor.segmentation_type_id in SegmentationDescriptor.ADVERTISEMENT_END:
                SCTE35_IN_QUEUE.append((id, specified_time))
      else:
        self.m3u8.push(packet)

      if PID == PCR_PID and ts.has_pcr(packet):
        PCR_VALUE = (cast(int, ts.pcr(packet)) - ts.HZ + ts.PCR_CYCLE) % ts.PCR_CYCLE
        PCR_DIFF = (
          (PCR_VALUE - LATEST_PCR_VALUE + ts.PCR_CYCLE) % ts.PCR_CYCLE
        ) if LATEST_PCR_VALUE is not None else 0
        LATEST_PCR_TIMESTAMP_90KHZ += PCR_DIFF
        if LATEST_PCR_DATETIME is None:
          LATEST_PCR_DATETIME = datetime.now(timezone.utc) - timedelta(seconds=(1))
        LATEST_PCR_DATETIME += timedelta(seconds=(PCR_DIFF / ts.HZ))
        LATEST_PCR_VALUE = PCR_VALUE


async def main():
  routes = web.RouteTableDef()
  loop = asyncio.get_running_loop()
  parser = argparse.ArgumentParser(description=('biim: LL-HLS origin'))

  parser.add_argument('-i', '--input', type=argparse.FileType('rb'), nargs='?', default=sys.stdin.buffer)
  parser.add_argument('-s', '--SID', type=int, nargs='?')
  parser.add_argument('-w', '--window_size', type=int, nargs='?')
  parser.add_argument('-t', '--target_duration', type=int, nargs='?', default=3.0)
  parser.add_argument('-p', '--part_duration', type=float, nargs='?', default=1.1)
  parser.add_argument('--port', type=int, nargs='?', default=8080)

  args = parser.parse_args()

  m3u8 = M3U8(
    target_duration=args.target_duration,
    part_target=args.part_duration,
    window_size=args.window_size,
  )

  @routes.get('/playlist.m3u8')
  async def _playlist(request: web.Request) -> web.Response:
    nonlocal m3u8

    msn = request.query['_HLS_msn'] if '_HLS_msn' in request.query else None
    part = request.query['_HLS_part'] if '_HLS_part' in request.query else None
    skip = request.query['_HLS_skip'] == 'YES' if '_HLS_skip' in request.query else False

    if msn is None and part is None:
      future = m3u8.plain()
      if future is None:
        return PlaylistResponse(status=400)
      result = await future
      return PlaylistResponse(text=result)
    else:
      if msn is None:
        return PlaylistResponse(status=400)
      msn = int(msn)
      if part is None:
        part = 0
      part = int(part)
      future = m3u8.blocking(msn, part, skip)
      if future is None:
        return PlaylistResponse(status=400)
      result = await future
      return PlaylistResponse(text=result)

  @routes.get('/segment-{msn:\d+}.ts')
  async def _segment(request: web.Request) -> web.Response | web.StreamResponse:
    nonlocal m3u8

    msn = int(request.match_info['msn'])

    # msn = request.query['msn'] if 'msn' in request.query else None
    # if msn is None:
    #   return InvalidMP4Response()

    msn = int(msn)
    queue = await m3u8.segment(msn)
    if queue is None:
      return InvalidMP4Response()

    response = StreamMP4Response(status=200)
    await response.prepare(request)

    while True:
      stream = await queue.get()
      if stream is None:
        break
      await response.write(stream)

    await response.write_eof()
    return response

  @routes.get('/part-{msn:\d+}-{part:\d}.ts')
  async def partial(request: web.Request) -> web.Response | web.StreamResponse:
    nonlocal m3u8

    msn = int(request.match_info['msn'])
    part = int(request.match_info['part'])
    # msn = request.query['msn'] if 'msn' in request.query else None
    # part = request.query['part'] if 'part' in request.query else None

    if msn is None:
      return InvalidMP4Response()
    msn = int(msn)

    if part is None:
      return InvalidMP4Response()
    part = int(part)

    queue = await m3u8.partial(msn, part)
    if queue is None:
      return InvalidMP4Response()

    response = StreamMP4Response(status=200)
    await response.prepare(request)

    while True:
      stream = await queue.get()
      if stream is None:
        break
      await response.write(stream)

    await response.write_eof()
    return response

  # Setup server
  app = web.Application()
  app.add_routes(routes)
  runner = web.AppRunner(app)
  await runner.setup()
  await loop.create_server(cast(web.Server, runner.server), '0.0.0.0', args.port)

  # Setup input parser
  input_stream_parser = InputStreamParser(
    input=args.input,
    m3u8=m3u8,
    sid=args.SID,
    part_duration=args.part_duration,
  )
  await input_stream_parser.connect()

  # Run continous parser
  await input_stream_parser.read()


if __name__ == '__main__':
  try:
    asyncio.run(main())
  except KeyboardInterrupt:
    print("Stopped.")
