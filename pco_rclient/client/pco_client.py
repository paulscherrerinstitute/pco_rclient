# -*- coding: utf-8 -*-
"""Client to control the gigafrost writer
"""

from enum import Enum
import inspect
import ipaddress
import itertools
import json
import os
import pprint
import re
import requests
import sys
import time
import zmq


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

def is_valid_ip_address(ip_address):
    """
    Check whether the supplied string is a valid IP address.

    The method will simply raise the exception from the ipaddress module if the
    address is not valid.

    Parameters
    ----------
    ip_address : str
        The string representation of the IP address.

    Returns
    -------
    ip_address : str
        The valid IP address in string representation.

    """

    if not type(ip_address) is type(u""):
        ip_address = ip_address.decode()
    ip = ipaddress.ip_address(ip_address)
    return str(ip)

def is_valid_network_address(network_address, protocol='tcp'):
    """
    Verify if a network address is valid.

    In the context of this method, a network addres must fulfill the following
    criteria to be valid:

    * It must start with a protocol specifier of the following form:
      "<PROT>://", where <PROT> can be any of the usual protocols.
      E.g.: "http://"
    * The protocol specifier is followed by a valid IP v4 address or a  host
      name (a host name must contain at least one alpha character)
    * The network address is terminated with a 4-5 digit port number preceeded
      by a colon. E.g.: ":8080"

    If all of these critera are met, the passed network address is returned
    again, otherwise the method returns None

    Parameters
    ----------
    network_address : str
        The network address to be verified.
    protocol : str, optional
        The network protocol that should be ascertained during the validity
        check. (default = 'tcp')

    """
    # ip v4 pattern with no leading zeros and values up to 255
    ip_pattern = ("(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}"
                  "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)")
    # hostname pattern requiring at least one alpha character (to distinguish
    # it from an ip address)
    hostname_pattern = "[\\w.\\-]*[^0-9.][\\w.\\-]*"
    # ports with 4 or 5 digits. No check is done for max port number of 65535
    port_pattern = ":[0-9]{4,5}"
    # protocol pattern end with "://". e.g.: "https://""
    protocol_pattern = "%s:[/]{2}" % protocol

    # check if address is given with an IP address
    ip_find_pattern = "(?<={}){}(?={})".format(
        protocol_pattern, ip_pattern, port_pattern)
    ip = re.findall(ip_find_pattern, network_address)
    if ip:
        ip = is_valid_ip_address(ip[0])
        connection_pattern = protocol_pattern + ip_pattern + port_pattern
        if bool(re.match(connection_pattern, network_address)):
            return network_address

    # check if address is given with a hostname
    hostname_find_pattern = "(?<={}){}(?={})".format(
        protocol_pattern, hostname_pattern, port_pattern)
    hostname = re.findall(hostname_find_pattern, network_address)
    if hostname:
        if bool(re.match(protocol_pattern + hostname_pattern + port_pattern,
                         network_address)):
            return network_address

    return None

def is_valid_connection_address(connection_address, name):
    addr = is_valid_network_address(connection_address, protocol='tcp')
    if addr:
        return addr
    else:
        raise PcoError("Problem with the {}}:\n  {} does not seem to be a "
                       "valid address".format(name, connection_address))

def is_valid_dataset_name(dataset_name, name):
    dataset_name = str(dataset_name)
    if len(dataset_name) > 0:
        return dataset_name
    else:
        raise PcoError("Problem with the %s parameter: "
                       "not a valid dataset name" % name)

def is_valid_nonneg_int_parameter(parameter_int, name):
    parameter_int = int(parameter_int)
    if parameter_int >= 0:
        return parameter_int
    else:
        raise PcoError("Problem with the %s parameter: "
                       "not a non-negative integer" % name)

def is_valid_output_file(output_file, name):
    if bool(re.match("[%./a-zA-Z0-9_-]*.h5", output_file)):
        return output_file
    else:
        raise PcoError("Problem with the output file name %s." % name)

def is_valid_rest_api_address(rest_api_address, name):
    addr = is_valid_network_address(rest_api_address, protocol='http')
    if addr:
        return addr
    else:
        raise PcoError("Problem with the {}:\n  {} does not seem to be a "
                       "valid address".format(name, rest_api_address))

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

    def __init__(self, output_file='', dataset_name='', n_frames=0,
                 connection_address='tcp://129.129.99.104:8080',
                 flask_api_address = "http://xbl-daq-32:9901",
                 writer_api_address = "http://xbl-daq-32:9555",
                 user_id=503, max_frames_per_file=20000, debug=False):
        self.connection_address = is_valid_connection_address(
            connection_address, 'connection_address')
        self.flask_api_address = is_valid_rest_api_address(
            flask_api_address, 'flask_api_address')
        self.writer_api_address = is_valid_rest_api_address(
            writer_api_address, 'writer_api_address')
        self.output_file = ''
        self.dataset_name = ''
        self.n_frames = -1
        self.user_id = -1
        self.max_frames_per_file = -1
        self.status = 'initialized'
        self.last_run_id = 0
        self.last_run_json = None
        self.configured = False

        if not debug:
            if not self.is_running():
                if output_file:
                    self.output_file = is_valid_output_file(
                        output_file, 'output_file')
                if dataset_name:
                    self.dataset_name = is_valid_dataset_name(
                        dataset_name, "dataset_name")
                if n_frames > 0:
                    self.n_frames = is_valid_nonneg_int_parameter(
                        n_frames, 'n_frames')
                if max_frames_per_file > 0:
                    self.max_frames_per_file = is_valid_nonneg_int_parameter(
                        max_frames_per_file, 'max_frames_per_file')
                if user_id >= 0:
                    self.user_id = is_valid_nonneg_int_parameter(
                        user_id,'user_id')
                self.configured = self.validate_configuration()
                if self.configured:
                    self.status = 'configured'
            else:
                raise RuntimeError("\n Writer configuration can not be "
                    "updated while the PCO writer is running. Please, stop() "
                    "the writer first and then change the configuration.\n")
        else:
            print("\nSetting debug configurations... \n")
            self.flask_api_address = is_valid_rest_api_address(
                "http://localhost:9901", 'fask_api_address')
            self.writer_api_address = is_valid_rest_api_address(
                "http://localhost:9555", 'writer_api_address')
            self.connection_address = is_valid_connection_address(
                "tcp://pc9808:9999", 'connection_address')
            self.output_file = is_valid_output_file(output_file, 'output_file')
            self.user_id = is_valid_nonneg_int_parameter(0, 'user_id')
            self.n_frames = is_valid_nonneg_int_parameter(n_frames, 'n_frames')
            self.dataset_name = dataset_name
            self.max_frames_per_file = is_valid_nonneg_int_parameter(
                max_frames_per_file, 'max_frames_per_file')
            self.configured = True
            self.status = 'configured'

        # Not sure this is a good way of doing this...
        #if self.max_frames_per_file != 20000:
        #    # if needed, it verifies if output_file has placeholder
        #    regexp = re.compile(r'%[\d]+d')
        #    if not regexp.search(self.output_file):
        #        self.output_file = insert_placeholder(
        #            self.output_file, len(self.output_file)-3)


    def configure(self, output_file=None, dataset_name=None, n_frames=None,
                  connection_address=None, user_id=None,
                  max_frames_per_file=None, verbose=False):
        if not self.is_running():
            if output_file is not None:
                self.output_file = is_valid_output_file(
                    output_file, 'output_file')
            if dataset_name is not None:
                self.dataset_name = dataset_name
            if n_frames is not None:
                self.n_frames = is_valid_nonneg_int_parameter(
                    n_frames, 'n_frames')
            if user_id is not None:
                self.user_id = is_valid_nonneg_int_parameter(
                    user_id, 'user_id')
            if max_frames_per_file is not None:
                self.max_frames_per_file = is_valid_nonneg_int_parameter(
                    max_frames_per_file, 'max_frames_per_file')
            if connection_address is not None:
                self.connection_address = is_valid_connection_address(
                    connection_address, 'connection_address')
            # sets configured and status initialized
            self.configured = self.validate_configuration()
            if self.configured:
                    self.status = 'configured'
            if verbose:
                print("\nUpdated PCO writer configuration:\n")
                pprint.pprint(self.get_configuration())
                conf_validity = "NOT valid"
                if self.configured:
                    conf_validity = "valid"
                print("The current configuration is {} for data "
                      "aquisition".format(conf_validity))
                print("\n")
        else:
            if verbose:
                print("\n Writer configuration can not be updated while PCO "
                      "writer is running. Please, stop() the writer to change "
                      "configuration.\n")
            return None

    # def set_last_configuration(self, verbose=False):
    #     """
    #     Is this used anywhere???
    #     """
    #     is_writer_running = self.is_running()
    #     if not is_writer_running:
    #         self.output_file = is_valid_output_file(
    #             self.output_file, 'output_file')
    #         self.dataset_name = self.dataset_name
    #         self.n_frames = is_valid_nonneg_int_parameter(
    #             self.n_frames, 'n_frames')
    #         self.user_id = is_valid_nonneg_int_parameter(
    #             self.user_id, 'user_id')
    #         self.max_frames_per_file = is_valid_nonneg_int_parameter(
    #             self.max_frames_per_file, 'max_frames_per_file')
    #         self.connection_address = is_valid_connection_address(
    #             self.connection_address, 'connection_address')
    #         # sets configured and status initialized
    #         self.configured = True
    #         self.status = 'initialized'
    #         if verbose:
    #             print("\nUpdated PCO writer configuration:\n")
    #             pprint.pprint(self.get_configuration())
    #             print("\n")
    #         # returns the status to indicate that it's initialized
    #         return self.status
    #     else:
    #         if verbose:
    #             print("\n Writer configuration can not be updated while PCO "
    #                   "writer is running. Please, stop() the writer to change "
    #                   "configuration.\n")
    #     return None

    def get_configuration(self, verbose=False):
        configuration_dict = {
            "connection_address" : self.connection_address,
            "output_file":self.output_file,
            "n_frames" : self.n_frames,
            "user_id" : self.user_id,
            "dataset_name" : self.dataset_name,
            "max_frames_per_file" : self.max_frames_per_file,
            "statistics_monitor_address": "tcp://*:8088",
            "rest_api_port": "9555",
            "n_modules": "1"
        }
        if verbose:
            print("\nPCO writer configuration:\n")
            pprint.pprint(configuration_dict)
            print("\n")
        return configuration_dict

    def start(self, verbose=False):
        """
        Start a new writer process
        """
        if not self.configured:
            raise PcoError("please configure the writer by calling the "
                "configure() command before you start()")

        # check if writer is running before starting it
        if not self.is_running():
            request_url = self.flask_api_address + ROUTES["start_pco"]
            try:
                response = requests.post(request_url,
                    data=json.dumps(self.get_configuration())).json()
                if validate_response(response):
                    self.last_run_id += 1
                    if verbose:
                        print("\nPCO writer trigger start successfully "
                              "submitted to the server.\n")
                else:
                    print("\nPCO writer trigger start failed. "
                          "Server response: %s\n" % (response))
            except Exception as e:
                raise PcoWarning(e)
        else:
            print("\nWriter is already running, impossible to start() "
                  "again.\n")
            return None

    def wait(self, verbose=False):
        """
        Wait for the writer to finish the writing process.
        """

        # check if writer is running before killing it
        if not self.is_running():
            print("\nWriter is not running, nothing to wait().\n")
            return None
        else:
            print("Waiting for the writer to finish")
            print("  (Press Ctrl-C to stop waiting)")
        spinner = itertools.cycle(['-', '/', '|', '\\'])
        msg = "Status:"
        sys.stdout.write(msg)
        sys.stdout.flush()
        try:
            while self.is_running():
                stats = self.get_statistics()
                msg = ("Status: {}, # of frames received : {}, # of frames "
                       "written: {} {}".format(stats['value'], stats['???'], stats['n_written_frames'],
                       (next(spinner))))
                sys.stdout.write('\r\033[K')
                sys.stdout.write(msg)
                sys.stdout.flush()
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

        if verbose:
            if not self.is_running():
                print("\nWriter is not running anymore, exiting wait().\n")
            else:
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
        """
        Stop the writer
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
                print("\nWriter is not running, impossible to stop(). "
                      "Please start it using the start() method.\n")
            return None

    def get_written_frames(self):
        request_url = self.writer_api_address + ROUTES["statistics"]
        try:
            response = requests.get(request_url).json()
            if validate_statistics_response(response):
                return response['n_written_frames']
        except Exception:
            return None

    def get_statistics(self, verbose=False):
        # check if writer is running before getting statistics
        if self.is_running():
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
            if self.last_run_id > 0:
                self.status = self.get_previous_status()
                if verbose:
                    print("\nWriter is not running, getting status from previous execution.\n")
            else:
                if verbose:
                    print("\nWriter has not run yet")
        return self.status

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

    def reset(self):
        """
        Reset the writer object.
        """

        self.kill()
        self.flush_cam_stream()
        self.last_run_id = 0
        self.last_run_json = None
        self.status = 'initialized'
        self.configured = self.validate_configuration()
        if self.configured:
            self.status = 'configured'

    def validate_configuration(self):
        """
        Validate that the current configuration parameters are valid and
        sufficient for an acquisition.

        Returns
        -------
        configuration_is_valid : bool
            Returns True if the configuration is valid, False otherwise.

        """

        try:
            assert self.output_file == is_valid_output_file(
                self.output_file, 'output_file')
            assert self.dataset_name == self.dataset_name
            assert self.n_frames == is_valid_nonneg_int_parameter(
                self.n_frames, 'n_frames')
            assert self.user_id == is_valid_nonneg_int_parameter(
                self.user_id, 'user_id')
            assert self.max_frames_per_file == is_valid_nonneg_int_parameter(
                self.max_frames_per_file, 'max_frames_per_file')
            assert self.connection_address == is_valid_connection_address(
                self.connection_address, 'connection_address')
            return True
        except:
            return False