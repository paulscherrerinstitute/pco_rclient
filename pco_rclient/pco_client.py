# -*- coding: utf-8 -*-
"""
Client to control the PCO writer.
"""

# Notes
# =====
# 1.) This module uses numpy style docstrings, as they display nicely even
#       on the command line (for example, when using ipython's interactive
#       shell).
#
#   See:
#     http://sphinx-doc.org/latest/ext/napoleon.html
#     https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt
#
#   Use the napoleon Sphinx extension to render the docstrings correctly with
#   Sphinx: 'sphinx.ext.napoleon'
#
# 2.) Member methods are ordered alphabetically.


__author__ = 'Leonardo Hax Damiani'
__date_created__ = '2020-08-20'
__credits__ = 'Christian M. Schlepuetz'
__copyright__ = 'Copyright (c) 2020, Paul Scherrer Institut'
__docformat__ = 'restructuredtext en'


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
    return string[:index] + "_%03d" + string[index:]

def validate_connection_address(connection_address, name):
    addr = validate_network_address(connection_address, protocol='tcp')
    if addr:
        return addr
    raise PcoError("Problem with the {}:\n  {} does not seem to be a "
                    "valid address".format(name, connection_address))

def validate_dataset_name(dataset_name, name):
    dataset_name = str(dataset_name)
    if len(dataset_name) > 0:
        return dataset_name
    raise PcoError("Problem with the %s parameter: "
                    "not a valid dataset name" % name)

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

def validate_kill_response(writer_response, verbose=False):
    if writer_response['status'] == "killed":
        if verbose:
            print(writer_response['status'])
        return True
    return False

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

def validate_nonneg_int_parameter(parameter_int, name):
    parameter_int = int(parameter_int)
    if parameter_int >= 0:
        return parameter_int
    raise PcoError("Problem with the %s parameter: "
                    "not a non-negative integer" % name)

def validate_output_file(output_file, name):
    output_file = os.path.expanduser(output_file)
    if bool(re.match("[%./a-zA-Z0-9_-]*.h5", output_file)):
        return output_file
    raise PcoError("Problem with the output file name %s." % name)

def validate_response(server_response, verbose=False):
    if not server_response['success']:
        return False
    return True

def validate_rest_api_address(rest_api_address, name):
    addr = validate_network_address(rest_api_address, protocol='http')
    if addr:
        return addr
    raise PcoError("Problem with the {}:\n  {} does not seem to be a "
                    "valid address".format(name, rest_api_address))

def validate_statistics_response(writer_response, verbose=False):
    return writer_response


# Rest API routes.
ROUTES = {
    "start_pco": "/start_pco_writer",
    "status":"/status",
    "statistics":"/statistics",
    "stop": "/stop",
    "kill": "/kill",
    "error": "/error",
    "server_log": "/server_log",
    "server_uptime": "/server_uptime",
    "ack": "/ack",
    "finished": "/finished"
}

class PcoWriter(object):
    """
    Proxy Class to control the PCO writer.
    """

    def __init__(self, output_file='', dataset_name='', n_frames=0,
                 connection_address='tcp://129.129.99.104:8080',
                 flask_api_address = "http://xbl-daq-32:9901",
                 writer_api_address = "http://xbl-daq-32:9555",
                 user_id=503, max_frames_per_file=20000, debug=False):
        """
        Initialize the PCO Writer object.
        """

        # Note: the tcp://129.129.99.104:8080 connection address corresponds
        #       to the 1G copper link on x02da-pco-4
        #       (last updated: 2020-09-31)
        
        self.flask_api_address = validate_rest_api_address(
            flask_api_address, 'flask_api_address')
        self.writer_api_address = validate_rest_api_address(
            writer_api_address, 'writer_api_address')

        if not self.is_connected():
            print("WARNING: The writer server is not responding!")
            print("A connection attempt with the following network address "
                  "failed:\n  {}".format(self.flask_api_address))

        # set default values for configuration items
        self.connection_address = ''
        self.output_file = ''
        self.dataset_name = ''
        self.n_frames = 0
        self.max_frames_per_file = 0
        self.user_id = -1

        # set some start values
        self.last_run_id = 0
        self.previous_statistics = None
        self.status = 'unconfigured'

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
                if self.validate_configuration():
                    self.assert_filenumber_placeholder()
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
            self.dataset_name = validate_dataset_name(
                dataset_name, "dataset_name")
            self.max_frames_per_file = validate_nonneg_int_parameter(
                max_frames_per_file, 'max_frames_per_file')
            self.status = 'configured'

    def __str__(self):
        return("Proxy Class to control the PCO writer. It communicates with "
               "the flask server running on xbl-daq-32 and the writer process "
               "service (pco_writer_1).")

    def assert_filenumber_placeholder(self):
        """
        Ensure that the output file name contains a file number placeholder if
        necessary.

        In case the total number of frames to be acquired is greater than the
        maximum number of frames per file, this method inserts a filenumber
        placeholder into the output filename, if it is not already present.

        """

        if self.max_frames_per_file <= self.n_frames:
            # regex matching a pattern of "%d" or "%Nd" where N can be any
            # number of digits
            regexp = re.compile(r'%(\d|)+d')
            if not regexp.search(self.output_file):
                self.output_file = insert_placeholder(
                    self.output_file, len(self.output_file)-3)

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
                self.dataset_name = validate_dataset_name(
                dataset_name, "dataset_name")
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
            if self.validate_configuration():
                    self.assert_filenumber_placeholder()
                    self.status = 'configured'
            if verbose:
                print("\nUpdated PCO writer configuration:\n")
                pprint.pprint(self.get_configuration())
                conf_validity = "NOT valid"
                if self.validate_configuration():
                    conf_validity = "valid"
                print("The current configuration is {} for data "
                      "aquisition".format(conf_validity))
                print("\n")
            return self.get_configuration()
        if verbose:
            print("\n Writer configuration can not be updated while PCO "
                    "writer is running. Please, stop() the writer to change "
                    "configuration.\n")
        return None

    def flush_cam_stream(self, timeout=500, verbose=False):
        """
        Flush the ZMQ stream.

        Parameters
        ----------
        timeout : float, optional
            The timeout [ms] before terminating the flush operation when no new
            data is reveived from the stream anymore. If set to a non-positive
            number, the flush operation will never halt by itself and needs to
            be terminated with a KeyboardInterrupt (Ctrl-C) signal.
            (default = 500)
        verbose : bool, optional
            Show verbose information durign the flushing operation.
            (default = False)

        """
        if not self.is_running():
            try:
                if verbose:
                    print("Flushing camera stream ... (Ctrl-C to stop)")
                    if timeout > 0:
                        print("Flush will terminate after {} ms of inactivity on "
                            "the data stream.".format(timeout))
                context = zmq.Context()
                socket = context.socket(zmq.PULL)
                socket.connect(self.connection_address)
                packets_counter = 0
                if timeout > 0:
                        receiving = bool(socket.poll(timeout))
                else:
                    receiving = True
                while receiving:
                    string = socket.recv()
                    if packets_counter % 2 != 1:
                        if verbose:
                            d = json.loads(string.decode())
                            print(packets_counter, d)
                    packets_counter+=1
                    if timeout > 0:
                        receiving = bool(socket.poll(timeout))
                socket.close()
                context.term()
            except KeyboardInterrupt:
                pass
            return packets_counter
        return None

    def get_configuration(self, verbose=False):
        configuration_dict = {
            "connection_address": self.connection_address,
            "output_file": self.output_file,
            "n_frames": str(self.n_frames),
            "user_id": str(self.user_id),
            "dataset_name": self.dataset_name,
            "max_frames_per_file": str(self.max_frames_per_file),
            "rest_api_port": "9555",
            "n_modules": "1"
        }
        if verbose:
            print("\nPCO writer configuration:\n")
            pprint.pprint(configuration_dict)
            print("\n")
        return configuration_dict

    def get_progress_message(self):
        """
        Return a string indicating the current progress of the writer.
        """

        stats = self.get_statistics()
        if stats is None:
            msg = "Writer: Status not available"
        else:
            status = stats.get('status', "unknown")
            n_req = stats.get('n_frames', -1)
            n_rcvd = stats.get('n_received_frames', 0)
            n_wrtn = stats.get('n_written_frames', 0)
            if n_req > 0: 
                pc_rcvd = float(n_rcvd) / n_req * 100.0
                pc_wrtn = float(n_wrtn) / n_req * 100.0
                msg = ("Writer: {}, #received: {:4d} ({:.1f}%), "
                        "#written: {:4d} ({:.1f}%)".format(
                        status, n_rcvd, pc_rcvd, n_wrtn, pc_wrtn))
            else:
                msg = ("Writer: {}, #received: {:4d}, "
                        "#written: {:4d}".format(
                        status, n_rcvd, n_wrtn))
        return msg

    def get_server_error(self, verbose=False):
        """
        Retrieve the the last error

        Returns
        -------
        error_msg : str
            The last error message from the writer or None (if not existant).

        """
        request_url = self.flask_api_address+ROUTES["error"]
        try:
            response = requests.get(request_url).json()
            if 'success' in response:
                if verbose:
                    print("\nPCO writer error:")
                    print(response)
                self.status = response['status']
                return response['error']
        except Exception as e:
            return None
        return None


    def get_server_log(self, verbose=False):
        """
        Retrieve the last 10 lines log of the writer server.

        Returns
        -------
        log : str
            The last 10 lines of the writer server.

        """
        request_url = self.flask_api_address+ROUTES["server_log"]
        try:
            response = requests.get(request_url).json()
            if 'success' in response:
                if verbose:
                    print("\nPCO writer server log:")
                    print(response['log'])
                return response['log']
        except Exception as e:
            return None
        return None

    def get_server_uptime(self, verbose=False):
        """
        Retrieves the uptime of the writer server service.

        Returns
        -------
        uptime : str
            The uptime of the writer server service.

        """
        request_url = self.flask_api_address+ROUTES["server_uptime"]
        try:
            response = requests.get(request_url, data={"key": "uptime"}).json()
            if 'success' in response:
                if verbose:
                    print("\nPCO writer server log:")
                    print("\t"+response['uptime'])
                return response['uptime']
        except Exception as e:
            return None
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

        stats = self.get_statistics_writer(verbose=verbose)
        if stats is None:
            stats = self.get_statistics_last_run(verbose=verbose)
        return stats

    def get_statistics_last_run(self, verbose=False):
        """
        Retrieve the statistics from the previous writer run.

        Returns
        -------
        stats : dict or None
            Returns the dictionary of statistics if request is successful,
            None otherwise.

        """

        request_url = self.flask_api_address+ROUTES["finished"]
        try:
            response = requests.get(request_url, timeout=3).json()
            self.previous_statistics = response
            if verbose:
                print("\nPCO writer statistics:\n")
                pprint.pprint(response)
                print("\n")
            return response
        except Exception as e:
            if verbose:
                print("PCO writer did not return a valid statistics "
                      "response for the previous run.")
            return None

    def get_statistics_writer(self, verbose=False):
        """
        Retrieve the statistics from a running writer process.

        Returns
        -------
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
            if verbose:
                print("PCO writer did not return a validated statistics "
                        "response")
            return None
        except requests.ConnectionError:
            # We expect a timeout error if the writer is not running, so return
            # None
            if verbose:
                print("PCO writer did not return a validated statistics "
                        "response")
            return None
        except Exception as e:
            # If the error was not a timeout (which is expected in this context
            # if the writer is actually not running), then it was probably more
            # serious and should be raised.
            if verbose:
                template = ("PCO writer did not return a validated statistics. "
                        "An exception of type {0} occurred. Arguments:\n{1!r}")
                print(template.format(type(e).__name__, e.args))
            raise PcoError(e)

    def get_status(self, verbose=False):
        """
        Gets the status of the PCO writer client instance.

        Returns
        -------
        status : str
            The current status of the client.
        """

        if self.is_running():
            # A writer process is currently running
            if not self.status in ('stopping', 'killing'):
                # return the status of the running writer
                status = self.get_status_writer()
                self.status = status
            else:
                # If we are trying to stop or kill the writer and its still
                # running, then report the fact that we are trying to stop/kill
                status = self.status
        elif self.status in ('receiving', 'writing'):
            # The last known status of the client was from a running writer
            # process, so check on the writer status and return that
            status = self.get_status_writer()
            self.status = status
        else:
            # Return the status of the client object itself.
            status = self.status
        return status

    def get_status_last_run(self):
        """
        Retrieve the status of the previous writer process.

        The writer server keeps track of the status of the last previously
        running writer process, which can be retrieved with this command.

        The previous status can be any one of the following:

            * "finished": The previous writer process finished successfully.
            * "unknown": The status of the previous writer process cannot be
              determined, or a writer process has never been run yet.

        Returns
        -------
        previous_status : str
            The status of the previous writer process.

        """

        request_url = self.flask_api_address+ROUTES["finished"]
        try:
            response = requests.get(request_url, timeout=3).json()
            return response['status']
        except requests.ConnectionError:
            raise PcoError("The writer server seems to be disconnected and is "
                           "not responding.")
        except:
            raise

    def get_status_writer(self):
        """
        Retrieve the status of the writer server.

        Returns
        -------
        status : str
            The current status of the running writer.

        """

        request_url = self.flask_api_address+ROUTES["status"]
        try:
            response = requests.get(request_url, timeout=3).json()
            return(response['status'])
        except requests.ConnectionError:
            raise PcoError("The writer server seems to be disconnected and is "
                           "not responding.")
        except:
            raise

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
            The number of frames written to file. Returns None if that
            information is not available.

        """

        stats = self.get_statistics()
        if stats is not None:
            return stats.get('n_written_frames', None)
        return None

    def is_connected(self):
        """
        Verify whether a connection to the writer service is available.
        """

        request_url = self.flask_api_address+ROUTES["ack"]
        try:
            response = requests.get(request_url, timeout=3).json()
            if 'success' in response:
                return True
        except requests.ConnectionError:
            return False
        return False

    def is_running(self):
        """
        Verify wether a writer process is currently running.
        """

        request_url = self.flask_api_address+ROUTES["status"]
        try:
            response = requests.get(request_url, timeout=3).json()
            return bool(response['status'] in ('receiving', 'writing'))
        except requests.ConnectionError:
            raise PcoError("The writer server seems to be disconnected and is "
                           "not responding.")
        except:
            raise

    def kill(self, verbose=False):
        """
        Kill the currently running writer process.

        Parameters
        ----------
        verbose : bool, optional
            Show verbose information during the kill process.
            (default = False)

        """

        # check if writer is running before killing it
        response = 0
        if self.is_running():
            self.status = 'killing'
            request_url = self.writer_api_address + ROUTES["kill"]
            try:
                response = requests.get(request_url).json()
                if validate_kill_response(response):
                    if verbose:
                        print("\nPCO writer process successfully killed.\n")
                else:
                    print("\nPCO writer kill() failed.")
            except requests.ConnectionError:
                raise PcoError("The writer server seems to be disconnected "
                               "and is not responding.")
            except:
                raise
        else:
            if verbose:
                print("\nWriter is not running, impossible to kill(). Please "
                      "start it using the start() method.\n")
        self.status = self.get_status()
        return response

    def reset(self):
        """
        Reset the writer client object.
        
        Returns
        -------
        status : str
            The current status of the client.
        """

        self.kill()
        self.flush_cam_stream(timeout=1000)
        self.last_run_id = 0
        self.previous_statistics = None
        if self.validate_configuration():
            self.status = 'configured'
        else:
            self.status = 'unconfigured'
        return self.status

    def start(self, wait=True, timeout=10, verbose=False):
        """
        Start a new writer process.

        Parameters
        ----------
        wait : bool, optional
            It waits for the writer to be running. (default = True)
        timeout : float, optional
            The maximum time [s] to wait for the writer to report a running
            status. (default = 10)
        verbose : bool, optional
            Show verbose information while starting the process.
            (default = False)

        """
        if not self.validate_configuration():
            raise PcoError("PCO writer is not properly configured! "
                "Please configure the writer by calling the "
                "configure() command before you start()")
        response = 0
        if not self.is_running():
            request_url = self.flask_api_address + ROUTES["start_pco"]
            try:
                self.status = 'starting'
                data_json = json.dumps(self.get_configuration())
                response = requests.post(request_url, data=data_json).json()
                if validate_response(response):
                    self.last_run_id += 1
                    if verbose:
                        print("\nPCO writer trigger start successfully "
                              "submitted to the server.\n")
                else:
                    if verbose:
                        print("\nPCO writer trigger start failed. "
                            "Server response: %s\n" % (response))
            except requests.ConnectionError:
                raise PcoError("The writer server seems to be disconnected "
                               "and is not responding.")
            except:
                raise
        else:
            if verbose: 
                print("\nWriter is already running, impossible to start() "
                    "again.\n")
        # waits for is_running if wait=True
        if 'success' in response and wait:
            timeout_limit = time.time() + timeout
            while not self.is_running():
                if time.time() > timeout_limit:
                    if verbose:
                        print("WARNING!\n"
                            "PCO writer did not report reaching the running "
                            "state within the timeout of {} s. ".format(timeout))
                    break
                time.sleep(0.15)
        self.status = self.get_status()
        return response

    def stop(self, wait=True, timeout=10,verbose=False):
        """
        Stop the writer process

        Parameters
        ----------
        verbose : bool, optional
            Show verbose information during the stopping process.
            (default = False)

        """

        # check if writer is running before stopping it
        response = 0
        if self.is_running():
            self.status = 'stopping'
            request_url = self.writer_api_address + ROUTES["stop"]
            try:
                response = requests.get(request_url).json()
                if validate_response(response):
                    if verbose:
                        print("\nPCO writer trigger stop successfully "
                              "submitted to the server.\n")
                else:
                    print("\nPCO writer stop writer failed. Server response: "
                          "%s\n" % (response))
            except requests.ConnectionError:
                raise PcoError("The writer server seems to be disconnected "
                               "and is not responding.")
            except:
                raise
        else:
            if verbose:
                print("\nWriter is not running, impossible to stop(). "
                      "Please start it using the start() method.\n")
        # waits for is_running if wait=True
        if response != 0:
            if 'success' in response and wait:
                timeout_limit = time.time() + timeout
                while self.is_running():
                    if time.time() > timeout_limit:
                        print("WARNING!\n"
                            "PCO writer did not report reaching the finished "
                            "state within the timeout of {} s. ".format(timeout))
                        break
                    time.sleep(0.15)
        self.status = self.get_status()
        return response

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
            assert self.dataset_name == validate_dataset_name(
                self.dataset_name, "dataset_name")
            assert self.n_frames == validate_nonneg_int_parameter(
                self.n_frames, 'n_frames')
            assert self.user_id == validate_nonneg_int_parameter(
                self.user_id, 'user_id')
            assert self.max_frames_per_file == validate_nonneg_int_parameter(
                self.max_frames_per_file, 'max_frames_per_file')
            assert self.connection_address == validate_connection_address(
                self.connection_address, 'connection_address')
            return True
        except Exception as e:
            return False

    def wait(self, verbose=False):
        """
        Wait for the writer to finish the writing process.

        Parameters
        ----------
        verbose : bool, optional
            Show verbose information during waiting.
            (default = False)

        """

        if not self.is_running():
            if verbose:
                print("\nWriter is not running, nothing to wait().\n")
            return

        if verbose:
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
        print("\n")
        self.status = self.get_status()

        if verbose:
            if not self.is_running():
                print("\nWriter is not running anymore, exiting wait().\n")
            else:
                print("\nWriter is still running, exiting wait().\n")

    def wait_nframes(self, nframes, inactivity_timeout=-1, verbose=False):
        """
        Wait for the writer to have written a given number of frames to file.

        This function is similar to the :meth:`wait` method, but returns as
        soon as a given number of frames have been processed. This number can
        be smaller than the total number of frames to be received by the
        writer. If the writer finishes before reaching this number, the wait is
        also finished.


        Parameters
        ----------
        nframes : int
            The number of frames to wait for.
        inactivity_timeout : float, optional
            If larger than zero, wait for so many seconds of writer inactivity
            (meaning that no new frames have been received or written to file)
            before giving up the wait. Set to a value <= 0 to disable this
            timeout and wait until the writer is not running anymore.
            (default = -1)
        verbose : bool, optional
            Show verbose information during waiting.
            (default = False)        
        
        Returns
        -------
        wait_success : bool
            True if the wait finished successfully, meaning it did not time
            out, False otherwise.

        """
        
        if not self.is_running():
            if verbose:
                print("\nWriter is not running, nothing to wait().\n")
            return

        if verbose:
            print("Waiting for the writer to process {} "
                  "frames".format(nframes))
            print("  (Press Ctrl-C to stop waiting)")
        spinner = itertools.cycle(['-', '/', '|', '\\'])
        nframes_proc = self.get_written_frames()
        perc_done = nframes_proc * 100.0 / nframes
        msg = ("Processed {} of {} frames ({:.1f}% done)".format(
            nframes_proc, nframes, perc_done))
        sys.stdout.write(msg)
        sys.stdout.flush()
        last_update_time = time.time()
        nframes_old = 0
        try:
            while nframes_proc < nframes:
                nframes_proc = self.get_written_frames()
                perc_done = nframes_proc * 100.0 / nframes
                msg = ("Processed {} of {} frames ({:.1f}% done)".format(
                    nframes_proc, nframes, perc_done))
                msg1 = ("{} {}".format(msg, (next(spinner))))
                sys.stdout.write('\r\033[K')
                sys.stdout.write(msg1)
                sys.stdout.flush()
                if nframes_proc > nframes_old:
                    nframes_old = nframes_proc
                    last_update_time = time.time()
                if ((inactivity_timeout > 0) and
                    (time.time() - last_update_time > inactivity_timeout)):
                    print("\n")
                    print(" *** WARNING: Writer did not receive all requested "
                        "images!")
                    print("     Giving up after {} seconds of inactivity "
                        "...").format(inactivity_timeout)
                    return(False)
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

        self.status = self.get_status()

        if verbose:
            if not self.is_running():
                print("\nWriter is not running anymore, exiting wait().\n")
            else:
                print("\nWriter is still running, exiting wait().\n")
        return True
