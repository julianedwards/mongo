import unittest
import jasper.jasper_pb2 as pb
import jasper.jasper_pb2_grpc as rpc

# pylint: disable=missing-docstring,protected-access


class TestLogsSplitter(unittest.TestCase):

   def test_logging_endpoint(self):
      level = pb.LogLevel(threshold=30, default=30)
      buffer_options = pb.BufferOptions(buffered=False, duration=0, max_size=0)
      base_options = pb.BaseOptions(level=level, buffer=buffer_options, format=pb.LOGFORMATPLAIN)
      file_logger = pb.FileLoggerOptions(filename='unittest.log', base=base_options)
      logger_config = pb.LoggerConfig()
      logger_config.file.CopyFrom(file_logger)
      create_options = pb.CreateOptions(args=['ls'], working_directory='/Users/julianedwards/mongo', output=pb.OutputOptions(loggers=[logger_config]))

      res = rpc.JasperProcessManager().Create(request=create_options, target="localhost")
