import subprocess
import time
import unittest
import grpc.experimental

from buildscripts.resmokelib import config
from buildscripts.resmokelib import run

# pylint: disable=missing-docstring,protected-access

class TestJasperLogging(unittest.TestCase):
    def test_logging_endpoint(self):
        runner = run.TestRunner("")
        curator_path = runner._get_jasper_reqs()

        import jasper.jasper_pb2 as pb
        import jasper.jasper_pb2_grpc as rpc

        jasper_port = 8000
        jasper_conn_str = "localhost:%d" % jasper_port
        jasper_command = [
            curator_path, "jasper", "service", "run", "rpc", "--port", str(jasper_port)
        ]
        jasper_service = subprocess.Popen(jasper_command)
        time.sleep(1)
        stub = rpc.JasperProcessManagerStub(grpc.insecure_channel(jasper_conn_str))

        level = pb.LogLevel(threshold=30, default=30)
        buildlogger_info = pb.BuildloggerV3Info(
                project="resmoke-unittest",
                task_id="test-jasper-proto",
                rpc_address="cedar.mongodb.com:8080"
        )
        buildlogger_options = pb.BuildloggerV3Options(buildloggerv3=buildlogger_info, level=level)
        logger_config = pb.LoggerConfig()
        logger_config.buildloggerv3.CopyFrom(buildlogger_options)
        create_options = pb.CreateOptions(args=['ls'], working_directory='.', output=pb.OutputOptions(loggers=[logger_config]))
        res = stub.Create(request=create_options)

        jasper_service.terminate()
