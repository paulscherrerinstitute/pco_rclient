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


def insert_placeholder(string, index):
    return string[:index] + "%03d" + string[index:]

def validate_ip_address(ip_address):
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
        The validated IP address in string representation.

    """

    if not type(ip_address) is type(u""):
        ip_address = ip_address.decode()
    ip = ipaddress.ip_address(ip_address)
    return str(ip)

def validate_network_address(network_address, protocol='tcp'):
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

    Returns
    -------
    net_addr : str or None
        The validated network address. If the validation failed, None is
        returned.

    """

    # ip v4 pattern with no leading zeros and values up to 255
    ip_pattern = ("(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}"
                  "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)")
    # hostname pattern requiring at least one (non-numeric AND non-period)-
    # character (to distinguish it from an ip address)
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
        try:
            ip = validate_ip_address(ip[0])
        except Exception as e:
            raise PcoWarning(e)
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

def validate_connection_address(connection_address, name):
    addr = validate_network_address(connection_address, protocol='tcp')
    if addr:
        return addr
    else:
        raise PcoError("Problem with the {}:\n  {} does not seem to be a "
                       "valid address".format(name, connection_address))

def validate_dataset_name(dataset_name, name):
    dataset_name = str(dataset_name)
    if len(dataset_name) > 0:
        return dataset_name
    else:
        raise PcoError("Problem with the %s parameter: "
                       "not a valid dataset name" % name)

def validate_nonneg_int_parameter(parameter_int, name):
    parameter_int = int(parameter_int)
    if parameter_int >= 0:
        return parameter_int
    else:
        raise PcoError("Problem with the %s parameter: "
                       "not a non-negative integer" % name)

def validate_output_file(output_file, name):
    if bool(re.match("[%./a-zA-Z0-9_-]*.h5", output_file)):
        return output_file
    else:
        raise PcoError("Problem with the output file name %s." % name)

def validate_rest_api_address(rest_api_address, name):
    addr = validate_network_address(rest_api_address, protocol='http')
    if addr:
        return addr
    else:
        raise PcoError("Problem with the {}:\n  {} does not seem to be a "
                       "valid address".format(name, rest_api_address))

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
    """
    Proxy Class to control the PCO writer.
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
        self.flask_api_address = validate_rest_api_address(
            flask_api_address, 'flask_api_address')
        self.writer_api_address = validate_rest_api_address(
            writer_api_address, 'writer_api_address')
        self.status = 'initialized'
        self.last_run_id = 0
        self.last_run_json = None
        self.configured = False

        if not debug:
            if not self.is_running():
                self.connection_address = validate_connection_address(
                    connection_address, 'connection_address')
                if output_file:
                    self.output_file = validate_output_file(
                        output_file, 'output_file')
                if dataset_name:
                    self.dataset_name = validate_dataset_name(
                        dataset_name, "dataset_name")
                if n_frames >= 0:
                    self.n_frames = validate_nonneg_int_parameter(
                        n_frames, 'n_frames')
                if max_frames_per_file > 0:
                    self.max_frames_per_file = validate_nonneg_int_parameter(
                        max_frames_per_file, 'max_frames_per_file')
                if user_id >= 0:
                    self.user_id = validate_nonneg_int_parameter(
                        user_id,'user_id')
                self.configured = self.validate_configuration()
                if self.configured:
                    self.status = 'configured'
            else:
                print("WARNING!\n The writer configuration could not be "
                    "fully applied because a PCO writer process is currently "
                    "running. Please stop() the writer process first and then "
                    "change the configuration using the configure() method.\n")
                print("Current configuration:")
                pprint.pprint(self.get_configuration())
        else:
            print("\nSetting debug configurations... \n")
            self.flask_api_address = validate_rest_api_address(
                "http://localhost:9901", 'fask_api_address')
            self.writer_api_address = validate_rest_api_address(
                "http://localhost:9555", 'writer_api_address')
            self.connection_address = validate_connection_address(
                "tcp://pc9808:9999", 'connection_address')
            self.output_file = validate_output_file(output_file, 'output_file')
            self.user_id = validate_nonneg_int_parameter(0, 'user_id')
            self.n_frames = validate_nonneg_int_parameter(n_frames, 'n_frames')
            self.dataset_name = dataset_name
            self.max_frames_per_file = validate_nonneg_int_parameter(
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
        """
        Configure the PCO writer for the next acquisition.

        Parameters
        ----------
        output_file : str, optional
            The output file name (or file name template) for the hdf5 file.
            Must end with ".h5" and can contain an optional format specifier
            such as "02d" which is used for the file numbering in case the
            frames are distributed over multiple files, see also the
            `max_frames_per_file` option. (default = None)
        dataset_name : str, optional
            The name of the dataset to be created in the hdf5 file. The dataset
            will be placed inside the "/exchange" group of the hdf5 file.
            (default = None)
        n_frames : int, optional
            The total number of frames to be written into the file(s). Must be
            a non-negative integer. (default = None)
        connection_address : str, optional
            The connection address for the zmq stream to the writer.
        user_id : int, optional
            The numeric user-ID of the user account used to write the data to
            disk. (default = None)
        max_frames_per_file : int, optional
            The maximum number of frames to store in a single hdf5 file. Eeach
            time the number of received frames exceedes this maximum number per
            file, a new file with an incremented file index (whose format can
            be controlled by including the corresponding format specifier in
            the `output_file` name) will be created. (default = None)
        verbose : bool, optional
            The verbosity level for the configure command. If verbose is True,
            the current configuration will printed at the end.
            (default = False)

        Returns
        -------
        conf : dict or None
            The current configuration after applying the requested
            configuration changes. None is returned if the configuration could
            not be updated (e.g., when the writer is already running).

        """

        if not self.is_running():
            if output_file is not None:
                self.output_file = validate_output_file(
                    output_file, 'output_file')
            if dataset_name is not None:
                self.dataset_name = dataset_name
            if n_frames is not None:
                self.n_frames = validate_nonneg_int_parameter(
                    n_frames, 'n_frames')
            if user_id is not None:
                self.user_id = validate_nonneg_int_parameter(
                    user_id, 'user_id')
            if max_frames_per_file is not None:
                self.max_frames_per_file = validate_nonneg_int_parameter(
                    max_frames_per_file, 'max_frames_per_file')
            if connection_address is not None:
                self.connection_address = validate_connection_address(
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
            return self.get_configuration()
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
    #         self.output_file = validate_output_file(
    #             self.output_file, 'output_file')
    #         self.dataset_name = self.dataset_name
    #         self.n_frames = validate_nonneg_int_parameter(
    #             self.n_frames, 'n_frames')
    #         self.user_id = validate_nonneg_int_parameter(
    #             self.user_id, 'user_id')
    #         self.max_frames_per_file = validate_nonneg_int_parameter(
    #             self.max_frames_per_file, 'max_frames_per_file')
    #         self.connection_address = validate_connection_address(
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
        if not self.validate_configuration():
            raise PcoError("PCO writer is not properly configured! "
                "Please configure the writer by calling the "
                "configure() command before you start()")

        # check if writer is running before starting it
        if not self.is_running():
            request_url = self.flask_api_address + ROUTES["start_pco"]
            try:
                self.status = 'starting'
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
                self.status = self.get_status()
            except Exception as e:
                raise PcoWarning(e)
        else:
            print("\nWriter is already running, impossible to start() "
                  "again.\n")

    def wait(self, verbose=False):
        """
        Wait for the writer to finish the writing process.
        """

        # check if writer is running before killing it
        if not self.is_running():
            print("\nWriter is not running, nothing to wait().\n")
            return

        print("Waiting for the writer to finish")
        print("  (Press Ctrl-C to stop waiting)")
        spinner = itertools.cycle(['-', '/', '|', '\\'])
        msg = self.get_progress_message()
        sys.stdout.write(msg)
        sys.stdout.flush()
        try:
            while self.is_running():
                msg = ("{} {}".format(self.get_progress_message(),
                                      (next(spinner))))
                sys.stdout.write('\r\033[K')
                sys.stdout.write(msg)
                sys.stdout.flush()
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

        self.status = self.get_status()

        if verbose:
            if not self.is_running():
                print("\nWriter is not running anymore, exiting wait().\n")
            else:
                print("\nWriter is still running, exiting wait().\n")

    def get_progress_message(self):
        """
        Return a string indicating the current progress of the writer.
        """

        stats = self.get_statistics()
        if stats is None:
            msg = "Status: not available"
        else:
            status = stats.get('value', "unknown")
            n_req = stats.get('n_frames', -1)
            n_rcvd = stats.get('n_received_frames', 0)
            n_wrtn = stats.get('n_written_frames', 0)
            pc_rcvd = float(n_rcvd) / n_req * 100.0
            pc_wrtn = float(n_wrtn) / n_req * 100.0
            msg = ("Status: {}, # of frames received : {} ({:.1f}%), "
                    "# of frames written: {} ({:.1f}%)".format(
                    status, n_rcvd, pc_rcvd, n_wrtn, pc_wrtn))
        return msg

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

    def stop(self, verbose=False):
        """
        Stop the writer process
        """
        # check if writer is running before killing it
        if self.is_running():
            self.status = 'stopping'
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
                self.status = self.get_status()
            except Exception as e:
                raise PcoError(e)
        else:
            if verbose:
                print("\nWriter is not running, impossible to stop(). "
                      "Please start it using the start() method.\n")

    def get_written_frames(self):
        """
        Return the number of frames written to file.

        In case a writer process is running, it returns the current number of
        that process. Otherwise, it reports the total number of frames written
        by the last writer process, or None if a writer process has never been
        running yet.

        Returns
        -------
        n_written_frames : int or None
            The number of frame written to file.

        """

        stats = self.get_statistics()
        if stats is not None:
            return stats.get('n_written_frames', None)
        else:
            return None

    def get_statistics(self, verbose=False):
        """
        Get the statistics of the writer.

        If the writer is currently running, the current statistics is
        returned, otherwise the statistics from the last writer process are
        returned.

        Returns
        -------
        stats : dict or None
            Returns the dictionary of statistics if request is successful
            (writer is running and responding), None otherwise.

        """

        stats = self.get_current_statistics(verbose=verbose)
        if stats is None:
            stats = self.get_previous_statistics(verbose=verbose)
        return stats

    def get_current_statistics(self, verbose=False):
        """
        Retrieve the statistics from a running writer process.

        Returns:
        --------
        stats : dict or None
            Returns the dictionary of statistics if request is successful
            (writer is running and responding), None otherwise.

        """

        request_url = self.writer_api_address + ROUTES["statistics"]
        try:
            response = requests.get(request_url).json()
            if validate_statistics_response(response):
                if verbose:
                    print("\nPCO writer statistics:\n")
                    pprint.pprint(response)
                    print("\n")
                return response
            else:
                if verbose:
                    print("PCO writer did not return a validated statistics "
                          "response")
                return None
        except TimeoutError:
            # We expect a timeout error if the writer is not running, so return
            # None
            return None
        except Exception as e:
            # If the error was not a timeout (which is expected in this context
            # if the writer is actually not running), then it was probably more
            # serious and should be raised.
            raise PcoError(e)

    def get_previous_statistics(self, verbose=False):
        """
        Retrieve the statistics from the previous writer run.

        Returns:
        --------
        stats : dict or None
            Returns the dictionary of statistics if request is successful,
            None otherwise.

        """

        request_url = self.flask_api_address+ROUTES["finished"]
        try:
            response = requests.get(request_url, timeout=3).json()
            self.last_run_json = response
            if verbose:
                print("\nPCO writer statistics:\n")
                pprint.pprint(response)
                print("\n")
            return response
        except:
            if verbose:
                print("PCO writer did not return a valid statistics "
                      "response for the previous run.")
            return None

    def get_status(self, verbose=False):
        """
        Return the status of the PCO writer instance and update its internal
        status attribute.
        """

        # Try if a status of a running writer process is available
        status = self.get_current_status()
        if status is None:
            # Writer process is currently not running
            if self.status in ('initialized', 'configured', 'starting'):
                # The writer has not yet run since its initialization or last
                # configuration change
                status = self.status
                if verbose:
                    print("\nWriter has not run yet")
            elif self.last_run_id > 0:
                # The writer is not running anymore, but did run before and has
                # not yet been reconfigured.
                status = self.get_previous_status()
                if verbose:
                    print("\nWriter is not running anymore, getting status "
                          "from the previous execution.\n")
            else:
                # Something went wrong somewhere, status is not known.
                status = "unknown"
        return status

    def get_current_status(self):
        """
        Retrieve the status of the running writer process.

        Returns
        -------
        status : str or None
            The current status of the running writer. If no writer process is
            running, None is returned.

        """

        request_url = self.flask_api_address+ROUTES["status"]
        try:
            response = requests.get(request_url, timeout=3).json()
            return(response['value'])
        except TimeoutError:
            # We expect a timeout error if the writer is not running, so return
            # None in this case.
            return(None)
        except Exception as e:
            # If the error was not a timeout (which is expected in this context
            # if the writer is actually not running), then it was probably more
            # serious and should be raised.
            raise PcoError(e)

    def get_previous_status(self):
        request_url = self.flask_api_address+ROUTES["finished"]
        try:
            response = requests.get(request_url, timeout=3).json()
            self.last_run_json = response
            status = response['value']
        except:
            status = 'unknown'
        return(status)

    def is_running(self):
        request_url = self.flask_api_address+ROUTES["status"]
        try:
            response = requests.get(request_url, timeout=3).json()
            if (response['value'] in ('receiving', 'writing')):
                return True
            else:
                return False
        except:
            return False

    def kill(self, verbose=False):
        # check if writer is running before killing it
        if self.is_running():
            self.status = 'killing'
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
                self.status = self.get_status()
            except Exception as e:
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
            assert self.output_file == validate_output_file(
                self.output_file, 'output_file')
            assert self.dataset_name == self.dataset_name
            assert self.n_frames == validate_nonneg_int_parameter(
                self.n_frames, 'n_frames')
            assert self.user_id == validate_nonneg_int_parameter(
                self.user_id, 'user_id')
            assert self.max_frames_per_file == validate_nonneg_int_parameter(
                self.max_frames_per_file, 'max_frames_per_file')
            assert self.connection_address == validate_connection_address(
                self.connection_address, 'connection_address')
            return True
        except:
            return False