
from epics import caput, caget
import sys
import time
import getpass
from datetime import datetime
import os
import re
import inspect
from itertools import ifilter
import unittest

# inserts the current client's code to path
sys.path.insert(0, '/sls/X02DA/data/e15741/git/pco_rclient/')
from pco_rclient import PcoWriter


def get_datetime_now():
    return datetime.now().strftime("%H%M%S")

###############################
#### SCRIPT USER VARIABLES ####
###############################
# test scope
test_no_writer = False
test_running_writer = True
# number of frames
nframes = 19 # total frames = nframes + 1 (starts at 0)
# defines the current time for the uniqueness of the output file
output_str = get_datetime_now()
# verboses 
VERBOSE = True
# user id
user_id = int(getpass.getuser()[1:])
# IOC's name
ioc_name = 'X02DA-CCDCAM2'
#ioc_name = 'X02DA-CCDCAM3'
# Output file path
outpath = "/sls/X02DA/data/e{}/Data10/pco_test/".format(user_id)
if not os.path.isdir(outpath):
    os.makedirs(outpath)

##########################################
#### CAMERA CONFIGURATION AND METHODS ####
##########################################
# IOC COMMANDS
COMMANDS = {
    "CAMERA":       ":CAMERA",
    "FILEFORMAT":   ":FILEFORMAT",
    "RECMODE":      ":RECMODE",
    "STOREMODE":    ":STOREMODE",
    "CLEARMEM":     ":CLEARMEM",
    "SET_PARAM":    ":SET_PARAM",
    "SAVESTOP":     ":SAVESTOP",
    "FTRANSFER":    ":FTRANSFER"
}

# combines the IOCNAME:CMD for a epics command (caput/caget)
def get_caput_cmd(ioc_name, command):
    return str(ioc_name+command)
# starts the camera transfer
def start_cam_transfer(n_frames=nframes):
    caput(get_caput_cmd(ioc_name, COMMANDS["SAVESTOP"]), n_frames) # Sets the number of frames to transfer
    caput(get_caput_cmd(ioc_name, COMMANDS["CAMERA"]), 1) # Starts the camera
    time.sleep(1)
    caput(get_caput_cmd(ioc_name, COMMANDS["FTRANSFER"]), 1) # Starts the transfer
# stops the camera transfer
def stop_cam_transfer():
    caput(get_caput_cmd(ioc_name, COMMANDS["CAMERA"]), 0) # Stops the camera
# configures the camera
def config_cam_transfer():
    caput(get_caput_cmd(ioc_name, COMMANDS["CAMERA"]), 0)
    caput(get_caput_cmd(ioc_name, COMMANDS["FILEFORMAT"]), 2)
    caput(get_caput_cmd(ioc_name, COMMANDS["RECMODE"]), 0)
    caput(get_caput_cmd(ioc_name, COMMANDS["STOREMODE"]), 1)
    caput(get_caput_cmd(ioc_name, COMMANDS["CLEARMEM"]), 1)
    caput(get_caput_cmd(ioc_name, COMMANDS["SET_PARAM"]), 1)

# configure the camera
config_cam_transfer()


class TestPcoClientMethods(unittest.TestCase):
    w = PcoWriter() 

    ####################################
    # TESTS BEFORE STARTING THE WRITER #
    ####################################
    
    def test_configure(self): 
        """
        Test configure method

        Asserts
        ----------
        status : str
            The status after the configure method must be 'configured'
        conf_dict : dict
            configure method returns the dictionary with the configuration
        """
        conf_dict = self.w.configure(output_file=os.path.join(
                                outpath, 'test'+output_str+'.h5'),
                                dataset_name="data", n_frames=nframes)
        self.assertEqual(self.w.get_status(), 'configured')
        self.assertNotEqual(conf_dict, None)
    
    def test_flush_cam_stream(self):
        """
        Test flush_cam_stream method

        Asserts
        ----------
        packets_counter : int
            each frame sends 2 packets. Asserts if the packets_counter is not 
            the exact double of the nframes (+1 because it starts at zero)
        """
        start_cam_transfer(nframes)
        packets_counter = self.w.flush_cam_stream(timeout=5000, verbose=False)
        stop_cam_transfer()
        self.assertEqual(packets_counter/2, nframes+1)

    def test_get_configuration(self):
        """
        Test get_configuration method

        Asserts
        ----------
        type : type(dict)
            Verifies if the return type is a dictionary
        configuration_dict : dict
            After setting a new configuration, it retrieves and compares both. 
            assertDictEqual verifies if both the predefined and the retrieved
            are identical
        """
        conf_dict = self.w.configure(output_file=os.path.join(
                                outpath, 'test'+output_str+'.h5'),
                                dataset_name="data_black", n_frames=nframes+10)
        configuration_dict = self.w.get_configuration()
        self.assertIsInstance(configuration_dict, dict)
        self.assertDictEqual(conf_dict, configuration_dict)
        

    def test_get_server_log(self):
        """
        Test get_server_log method

        Asserts
        ----------
        type : type(str)
            Verifies if the return type is a str
        log : str
            10 lines of the log are retrieved. it verifies how many occurances
            of the hostname (xbl-daq-32) were present in the log 
        """        
        # returns the last 10 lines of the server log
        log_str = str(self.w.get_server_log())
        # counts how many existances of a substring exists on the log
        count = log_str.count('xbl-daq-32.psi.ch')
        # is instance str
        self.assertIsInstance(log_str, str)
        # 10 lines -> 10 times xbl-daq-32.psi.ch (server's hostname)
        self.assertEqual(count, 10)
        
    def test_get_server_uptime(self):
        """
        Test get_server_uptime method

        Asserts
        ----------
        type : type(str)
            Verifies if the return type is a str
        uptime : str
            verifies if 'active' is present on the uptime
        """        
        # returns the last 10 lines of the server log
        uptime = str(self.w.get_server_uptime())
        # is instance str
        self.assertIsInstance(uptime, str)
        # assert if uptime contains 'active'
        self.assertTrue('active' in uptime)

    def test_get_statistics(self):
        """
        Test get_statistics method

        Asserts
        ----------
        type : type(dict)
            Verifies if the return type is a dict
        key : str
            verifies if all keys are present on the statistics dict
        """     
        ref_stats = {'status': 'finished', 'user_id': '15741', 'success': True, 'n_written_frames': '20', 'writing_rate': '3.5906642728904847', 'start_time': 'Fri Oct  2 08:37:01 2020\n', 'output_file': '/sls/X02DA/data/e15741/Data10/pco_test/test083700.h5', 'first_frame_id': '866', 'n_frames': '20', 'n_lost_frames': '0', 'duration_sec': '5.5700000000000003', 'end_time': 'Fri Oct  2 08:37:06 2020\n', 'dataset_name': 'data'}
        stats_dict = self.w.get_statistics()
        for key in ref_stats:
            value = stats_dict.get(key, None)
            self.assertNotEqual(value, None)
        
    def test_get_statistics_last_run(self):
        """
        Test get_statistics_last_run method

        Asserts
        ----------
        type : type(dict)
            Verifies if the return type is a dict
        key : str
            verifies if all keys are present on the statistics last run dict
        """     
        ref_stats_last_run = {'status': 'finished', 'user_id': '15741', 'success': True, 'n_written_frames': '20', 'writing_rate': '3.5906642728904847', 'start_time': 'Fri Oct  2 08:37:01 2020\n', 'output_file': '/sls/X02DA/data/e15741/Data10/pco_test/test083700.h5', 'first_frame_id': '866', 'n_frames': '20', 'n_lost_frames': '0', 'duration_sec': '5.5700000000000003', 'end_time': 'Fri Oct  2 08:37:06 2020\n', 'dataset_name': 'data'}
        stats_dict = self.w.get_statistics_last_run()
        for key in ref_stats_last_run:
            value = stats_dict.get(key, None)
            self.assertNotEqual(value, None)

    def test_get_statistics_writer(self):
        """
        Test get_statistics_writer method

        Asserts
        ----------
        stats_dict : dict
            while the writer is not running, get_statistics_writer returns None
        """     
        stats_dict = self.w.get_statistics_writer()
        self.assertEqual(stats_dict, None)

    def test_get_status(self):
        """
        Test get_status method

        Asserts
        ----------
        status : str
            returns a string (initialized, configured, unconfigured, writing, 
            receiving, finished, unknown, stopping, killing)
        """     
        status = self.w.get_status()
        self.assertTrue(bool(re.match("^[a-z]+$",status)))

    def test_get_status_last_run(self):
        """
        Test get_status_last_run method

        Asserts
        ----------
        status : str
            returns a string (initialized, configured, unconfigured, writing, 
            receiving, finished, unknown, stopping, killing)
        """     
        
        status = str(self.w.get_status_last_run())
        self.assertTrue(bool(re.match("^[a-z]+$",status)))

    def test_get_status_writer(self):
        """
        Test get_status_writer method

        Asserts
        ----------
        status : str
            returns a string (initialized, configured, unconfigured, writing, 
            receiving, finished, unknown, stopping, killing)
        """     
        # with the writer not running -> status = 'unknown'
        status = str(self.w.get_status_writer())
        self.assertEqual('unknown',status)

    def test_get_written_frames(self):
        """
        Test get_written_frames method

        Asserts
        ----------
        written_frames : int
            verifies if written_frames is integer
        """     
        # with the writer not running -> written_frames from previous run
        written_frames = self.w.get_written_frames()
        uni_or_str = isinstance(written_frames, unicode) or isinstance(written_frames, str)
        # if the information is not available, returns None
        self.assertTrue(uni_or_str or (written_frames is None))

    def test_is_connected(self):
        """
        Test is_connected method

        Asserts
        ----------
        is_connected : bool
            verifies if there's an active connection to the flask server
        """     
        # with the writer not running -> written_frames from previous run
        is_connected = self.w.is_connected()
        # True or False
        self.assertTrue(isinstance(is_connected, bool))

    def test_is_running(self):
        """
        Test is_running method

        Asserts
        ----------
        is_running : bool
            verifies if the writer is running
        """     
        # with the writer not running -> written_frames from previous run
        is_running = self.w.is_running()
        # True or False
        self.assertTrue(isinstance(is_running, bool))

    def test_kill(self):
        """
        Test kill method

        Asserts
        ----------
        ret_kill : int
            Kill method returns 0 if the writer is not running
        """     
        # with the writer not running -> written_frames from previous run
        ret_kill = self.w.kill()
        # writer is not running -> returns 0
        self.assertEqual(ret_kill, 0)

    def test_reset(self):
        """
        Test reset method

        Asserts
        ----------
        reset_status : int
            Reset method returns the status mode 
        """     
        # with the writer not running -> written_frames from previous run
        status = self.w.reset()
        # possible status
        possible_status = ['configured', 'unconfigured']
        # writer is not running -> returns 0
        self.assertTrue(status in possible_status)

    def test_stop(self):
        """
        Test stop method

        Asserts
        ----------
        ret_stop : int
            Stop method returns 0 if the writer is not running
        """     
        # with the writer not running -> written_frames from previous run
        ret_stop = self.w.stop()
        # writer is not running -> returns 0
        self.assertEqual(ret_stop, 0)


if __name__ == '__main__':
    w = PcoWriter(connection_address="tcp://129.129.99.104:8080", 
                           user_id=user_id)
    TestPcoClientMethods.w = w
    unittest.main()
