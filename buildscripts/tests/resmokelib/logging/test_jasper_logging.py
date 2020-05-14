import unittest
import jasper.jasper_pb2 as pb
import jasper.jasper_pb2_grpc as rpc

# pylint: disable=missing-docstring,protected-access


class TestLogsSplitter(unittest.TestCase):

   def test_logging_endpoint(self):
      level = pb.LogLevel(threshold=30, default=30)
      buffer = pb.BufferOptions(buffered=False, duration=0, max_size=0)
      base_options = pb.BaseOptions(level=level, buffer=buffer, format=pb.LOGFORMATPLAIN)
      file_logger = pb.LoggerConfig(file=pb.FileLoggerOptions(filename='unittest.log', base=base_options))
      create_options = pb.CreateOptions(args=['ls'], working_directory='/Users/guo/mongo', output=pb.OutputOptions(loggers=[file_logger]))

      res = rpc.JasperProcessManager().Create(request=create_options, target="localhost")
