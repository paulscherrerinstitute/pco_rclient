# -*- coding: utf-8 -*-
"""Client to control the gigafrost writer
"""

import os
import requests
import re
import json
import pprint
import sys
import time
import itertools
import zmq
import inspect
from enum import Enum

class NoTraceBackWithLineNumber(Exception):
    def __init__(self, msg):
        if (type(msg).__name__ == "ConnectionError" or
            type(msg).__name__ == "ReadTimeout"):
            print("\n ConnectionError/ReadTimeout: it seems that the server "
                  "is not running (check xbl-daq-32 pco writer service "
                  "(pco_writer_1), ports, etc).\n")
        try:
            ln = sys.exc_info()[-1].tb_lineno
        except AttributeError:
            ln = inspect.currentframe().f_back.f_lineno
        self.args = "{0.__name__} (line {1}): {2}".format(type(self), ln, msg),
        sys.tracebacklimit = None
        return None

class PcoError(NoTraceBackWithLineNumber):
    pass

class PcoWarning(NoTraceBackWithLineNumber):
    pass

def is_valid_connection_address(connection_address):
    if bool(re.match("tcp:[/]{2}[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}:[0-9]{4,5}", connection_address)):
        return connection_address
    elif bool(re.match("tcp:[/]{2}\w*:[0-9]{4}", connection_address)):
        return connection_address
    else:
        raise PcoError("Problem with the connection address.")

def is_valid_int_parameter(parameter_int, name):
    parameter_int = int(parameter_int)
    if isinstance(parameter_int, int) and parameter_int >= 0:
        return parameter_int
    else:
        raise PcoError("Problem with the %s parameter" % name)

def is_valid_output_file(output_file, name):
    if bool(re.match("[%./a-zA-Z0-9_-]*.h5", output_file)):
        return output_file
    else:
        raise PcoError("Problem with the output file name %s." % name)

def is_valid_rest_api_address(rest_api_address, name):
    if bool(re.match("http:[/]{2}[a-zA-Z0-9_-]*:[0-9]{4}", rest_api_address)):
        return rest_api_address
    else:
        raise PcoError("Problem with the rest api address %s." % name)

def insert_placeholder(string, index):
    return string[:index] + "%03d" + string[index:]

def validate_statistics_response(writer_response, verbose=False):
    return writer_response

def validate_response(server_response, verbose=False):
    if not server_response['success']:
        return False
    return True

def validate_kill_response(writer_response, verbose=False):
    if writer_response['status'] == "killed":
        if verbose:
            print(writer_response['status'])
        return True
    else:
        return False

# Rest API routes.
ROUTES = {
    "start_pco": "/start_pco_writer",
    "status":"/status",
    "statistics":"/statistics",
    "stop": "/stop",
    "kill": "/kill",
    "finished": "/finished"
}

class PcoPcoError(Exception):
    pass

class PcoWriter(object):
    """Proxy Class to control the PCO writer
    """
    def __str__(self):
        return("Proxy Class to control the PCO writer. It communicates with "
               "the flask server running on xbl-daq-32 and the writer process "
               "service (pco_writer_1).")

    def __init__(self, output_file='tmp.h5', dataset_name='data', 
                connection_address='tcp://129.129.99.104:8080', n_frames=0,
                user_id=503, max_frames_per_file=20000, debug=False):
        self.connection_address = is_valid_connection_address(connection_address)
        self.flask_api_address = is_valid_rest_api_address(
            "http://xbl-daq-32:9901", 'flask_api_address')
        self.writer_api_address = is_valid_rest_api_address(
            "http://xbl-daq-32:9555", 'writer_api_address')
        self.status = 'initialized'
        self.last_run_json = None
        self.configured = False
        if not debug:
            is_writer_running = self.is_running()
            if not is_writer_running:
                self.output_file = is_valid_output_file(output_file, 'output_file')
                self.dataset_name = dataset_name
                self.n_frames = is_valid_int_parameter(n_frames, 'n_frames')
                self.max_frames_per_file = is_valid_int_parameter(
                    max_frames_per_file, 'max_frames_per_file')
                self.user_id = is_valid_int_parameter(user_id,'user_id')
                self.configured = True
            else:
                raise RuntimeError("\n Writer configuration can not be updated "
                    "while the PCO writer is running. Please, stop() the writer "
                    " first and then change the configuration.\n")
        else:
            print("\nSetting debug configurations... \n")
            self.flask_api_address = is_valid_rest_api_address("http://localhost:9901", 'fask_api_address')
            self.writer_api_address = is_valid_rest_api_address("http://localhost:9555", 'writer_api_address')
            self.connection_address = is_valid_connection_address("tcp://pc9808:9999")
            self.output_file = is_valid_output_file(output_file, 'output_file')
            self.user_id = is_valid_int_parameter(0, 'user_id')
            self.n_frames = is_valid_int_parameter(n_frames, 'n_frames')
            self.dataset_name = dataset_name
            self.max_frames_per_file = is_valid_int_parameter(max_frames_per_file, 'max_frames_per_file')
            self.configured = True
        if self.max_frames_per_file != 20000:
            # if needed, it verifies if output_file has placeholder
            regexp = re.compile(r'%[\d]+d')
            if not regexp.search(self.output_file):
                self.output_file = insert_placeholder(self.output_file, len(self.output_file)-3)


    def configure(self, output_file=None, dataset_name=None, n_frames=None, connection_address=None,
                user_id=None, max_frames_per_file=None, verbose=False):
        is_writer_running = self.is_running()
        if not is_writer_running:
            if output_file is not None:
                self.output_file = is_valid_output_file(output_file, 'output_file')
            if dataset_name is not None:
                self.dataset_name = dataset_name
            if n_frames is not None:
                self.n_frames = is_valid_int_parameter(n_frames, 'n_frames')
            if user_id is not None:
                self.user_id = is_valid_int_parameter(user_id, 'user_id')
            if max_frames_per_file is not None:
                self.max_frames_per_file = is_valid_int_parameter(max_frames_per_file, 'max_frames_per_file')
            if connection_address is not None:
                self.connection_address = is_valid_connection_address(connection_address)
            # sets configured and status initialized
            self.configured = True
            self.status = 'initialized'
            if verbose:
                print("\nUpdated PCO writer configuration:\n")
                pprint.pprint(self.get_configuration())
                print("\n")
        else:
            if verbose:
                print("\n Writer configuration can not be updated while PCO "
                      "writer is running. Please, stop() the writer to change "
                      "configuration.\n")
            return None
       
    def set_last_configuration(self, verbose=False):
        is_writer_running = self.is_running()
        if not is_writer_running:
            self.output_file = is_valid_output_file(self.output_file, 'output_file')
            self.dataset_name = self.dataset_name
            self.n_frames = is_valid_int_parameter(self.n_frames, 'n_frames')
            self.user_id = is_valid_int_parameter(self.user_id, 'user_id')
            self.max_frames_per_file = is_valid_int_parameter(self.max_frames_per_file, 'max_frames_per_file')
            self.connection_address = is_valid_connection_address(self.connection_address)
            # sets configured and status initialized
            self.configured = True
            self.status = 'initialized'
            if verbose:
                print("\nUpdated PCO writer configuration:\n")
                pprint.pprint(self.get_configuration())
                print("\n")
            # returns the status to indicate that it's initialized
            return self.status
        else:
            if verbose:
                print("\n Writer configuration can not be updated while PCO "
                      "writer is running. Please, stop() the writer to change "
                      "configuration.\n")
        return None

    def get_configuration(self, verbose=False):
        if (self.status =='initialized' or self.status == 'finished') \
                and self.configured:
            configuration_dict = {
                "connection_address" : self.connection_address,
                "output_file":self.output_file,
                "n_frames" : str(self.n_frames),
                "user_id" : str(self.user_id),
                "dataset_name" : self.dataset_name,
                "max_frames_per_file" : str(self.max_frames_per_file),
                "statistics_monitor_address": "tcp://*:8088",
                "rest_api_port": "9555",
                "n_modules": "1"
            }
            if verbose:
                print("\nPCO writer configuration:\n")
                pprint.pprint(configuration_dict)
                print("\n")
            return configuration_dict
        else:
            return None

    def start(self, verbose=False):
        """start a new writer process
        """
        if self.status != 'initialized' and not self.configured:
            raise PcoError("please configure the writer by calling the "
                "configure() command before you start()")
        
        # check if writer is running before starting it
        if not self.is_running():
            request_url = self.flask_api_address + ROUTES["start_pco"]
            try:
                response = requests.post(request_url,
                    data=json.dumps(self.get_configuration())).json()
                if validate_response(response):
                    if verbose:
                        #giving some time before returning to the cli - why?
                        time.sleep(1)
                        print("\nPCO writer trigger start successfully "
                              "submitted to the server.\n")
                else:
                    print("\nPCO writer trigger start failed. "
                          "Server response: %s\n" % (response))
            except Exception as e:
                raise PcoWarning(e)
        else:
            print("\nWriter is already running, impossible to start_writer() "
                  "again. Getting the status of the writer... \n")
            return None

    def wait(self, verbose=False):
        """Wait for the writer to finish the writing process.
        """

        # check if writer is running before killing it
        is_writer_running = self.is_running()
        if not is_writer_running:
            print("\nWriter is not running, nothing to wait().\n")
            return None
        spinner = itertools.cycle(['-', '/', '|', '\\'])
        try:
            while is_writer_running:
                if not is_writer_running:
                    break
                else:
                    n_written_frames = self.get_written_frames()
                for _ in range(50):
                    msg = "Writer is running ... Current number of written frames: %s %s (Ctrl-C to stop waiting)" % ((str(n_written_frames)),(next(spinner)))
                    sys.stdout.write(msg)
                    sys.stdout.flush()
                    time.sleep(0.1)
                    sys.stdout.write('\r')
                    sys.stdout.flush()
                is_writer_running = self.is_running()
        except KeyboardInterrupt:
            pass
        if not is_writer_running and verbose:
            print("\nWriter is not running anymore, exiting wait().\n")
        elif verbose:
            print("\nWriter is still running, exiting wait().\n")
        return None
        

    def flush_cam_stream(self, verbose=False):
        try:
            if verbose:
                print("Flushing camera stream ... (Ctrl-C to stop)")
            context = zmq.Context()
            socket = context.socket(zmq.PULL)
            socket.connect(self.connection_address)
            x = 0
            while True:
                string = socket.recv()
                if x % 2 != 1:
                    d = json.loads(string.decode())
                    if verbose:
                        print(d)
                x+=1
            socket.close()
            context.term()
        except KeyboardInterrupt:
            pass
        return

    def stop(self, verbose=False):
        """stop the writer 
        """
        # check if writer is running before killing it
        if self.is_running():
            request_url = self.writer_api_address + ROUTES["stop"]
            try:
                response = requests.get(request_url).json()
                if validate_response(response):
                    if verbose:
                        print("\nPCO writer trigger stop successfully "
                              "submitted to the server.\n")
                    self.configured = False

                else:
                    print("\nPCO writer stop writer failed. Server response: "
                          "%s\n" % (response))
            except Exception as e:
                raise PcoError(e)
        else:
            if verbose:
                print("\nWriter is not running, impossible to stop_writer(). "
                      "Please start it using the start_writer() method.\n")
            return None

    def get_written_frames(self):
        request_url = self.writer_api_address + ROUTES["statistics"]
        try:
            response = requests.get(request_url).json()
            if validate_statistics_response(response):
                return response['n_written_frames']
        except Exception as e:
            return None


    def get_statistics(self, verbose=False):
        # check if writer is running before getting statistics
        is_writer_running = self.is_running()
        if is_writer_running:
            request_url = self.writer_api_address + ROUTES["statistics"]
            try:
                response = requests.get(request_url).json()
                if validate_statistics_response(response):
                    if verbose:
                        print("\nPCO writer statistics:\n")
                        pprint.pprint(response)
                        print("\n")
                    return response
            except Exception as e:
                raise PcoError(e)
        elif self.get_previous_status() == 'finished': #gets last statistics
            if verbose:
                print("\nWriter is not running, getting statistics "
                    "from previous execution.\n")
            return self.get_previous_statistics()
        return None

    def get_previous_status(self):
        request_url = self.flask_api_address+ROUTES["finished"]
        try:
            response = requests.get(request_url, timeout=3).json()
            self.last_run_json = response
            self.status = response['value']
        except:
            self.status = 'unknown'
        return self.status

    def get_previous_statistics(self):
        request_url = self.flask_api_address+ROUTES["finished"]
        try:
            response = requests.get(request_url, timeout=3).json()
            self.last_run_json = response
            self.status = response['value']
            return self.last_run_json
        except:
            self.status = 'unknown'
            return self.last_run_json
                

    def get_status(self, verbose=False):
        # check if writer is running: defines if current / previous status is requested
        if self.is_running():
            request_url = self.flask_api_address+ROUTES["status"]
            try:
                response = requests.get(request_url, timeout=3).json()
                self.status = response['value']
                return self.status
            except:
                self.status = 'unknown'
                return self.status
        else: # writer is not running
            if verbose:
                print("\nWriter is not running, getting status from previous execution.\n")
            return self.get_previous_status()

    def is_running(self):
        request_url = self.flask_api_address+ROUTES["status"]
        try:
            response = requests.get(request_url, timeout=3).json()
            if (response['value'] in ['receiving', 'writing']):
                return True
            else:
                return False
        except:
            self.status = 'unknown'
            return False

    def kill(self, verbose=False):
        # check if writer is running before killing it
        if self.is_running():
            request_url = self.writer_api_address + ROUTES["kill"]
            try:
                response = requests.get(request_url).json()
                if validate_kill_response(response):
                    if verbose:
                        print("\nPCO writer process successfully killed.\n")
                    self.status = 'finished'
                    self.configured = False
                else:
                    print("\nPCO writer kill() failed.")
            except Exception as e:
                self.status = 'unknown'
                raise PcoError(e)
        else:
            if verbose:
                print("\nWriter is not running, impossible to kill(). Please "
                      "start it using the start() method.\n")
            return None
