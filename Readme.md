[![CodeFactor](https://www.codefactor.io/repository/github/paulscherrerinstitute/pco_rclient/badge)](https://www.codefactor.io/repository/github/paulscherrerinstitute/pco_rclient) ![GitHub Release Date](https://img.shields.io/github/release-date/paulscherrerinstitute/pco_rclient) ![release](https://img.shields.io/github/v/release/paulscherrerinstitute/pco_rclient) ![language](https://img.shields.io/github/languages/top/paulscherrerinstitute/pco_rclient)


# Overview
This is the python client for the pco cameras running at Tomcat beamline (Paul Scherrer Institute). It allows one to perform operations on the PCO cameras. 


![architecture](https://github.com/paulscherrerinstitute/lib_cpp_h5_writer/raw/tomcat/docs/pco_diagram.jpg)



# Usage

## pco_controller via python script
The pco_controller is meant for flexible usage and control of the pco writer from within python scripts. 


```python
    pco_controller = PcoWriter(connection_address="tcp://129.129.99.104:8080", 
                    user_id=user_id, output_file='test.h5', dataset_name="data", n_frames=nframes)
```

Parameters:

| Name  |  Description  |
|---|---|
| output_file  | Output file name.  |
| dataset_name  | Dataset name (data, data_black, data_white)  |
| n_frames  | Total number of frames expected.  |
| connection_address  | Address of the camera server, where the incoming ZMQ stream is generated (tcp://129.129.99.104:8080)   |
| flask_api_address  | Address of the flask server (http://xbl-daq-32:9901)  |
| writer_api_address  | Address of the writer (http://xbl-daq-32:9555)  |
| user_id  | User id  |
| max_frames_per_file  | Defines the max frames on each file (h5 output with multiple chunked files)  |
| debug  | Runs the client on a local debug configuration.  |


# Installation

To create a new conda environment with the package installed:
```bash
conda create --name <env-name> -c paulscherrerinstitute pco_rclient
```

To install the package on a previously existing conda environment:
```bash
conda install -c paulscherrerinstitute pco_rclient
```

# Methods:

| Name  |  Description  | Parameters |
|---|---|---|
| PcoWriter  | Constructor method, Initialize the PCO Writer object.  | output_file='', dataset_name='', n_frames=0, connection_address='tcp://129.129.99.104:8080', flask_api_address = "http://xbl-daq-32:9901", writer_api_address = "http://xbl-daq-32:9555", user_id=503, max_frames_per_file=20000, debug=False |
| configure  | Configure the PCO writer for the next acquisition.  | output_file=None, dataset_name=None, n_frames=None, connection_address=None, user_id=None, max_frames_per_file=None, verbose=False |
| flush_cam_stream  | Flush the ZMQ stream.  | timeout=500, verbose=False |
| get_configuration  | Retrieve the current client's configuration. | verbose=False |
| get_server_error | Retrieve the last error (if any) from the server. | verbose=False |
| get_server_log | Retrieve the log from the server. | verbose=False |
| get_server_uptime | Retrieves the uptime of the writer server service.. | verbose=False |
| get_statistics  | Get the statistics of the writer (or previous, if the writer is not running) | verbose=False |
| get_statistics_last_run | Retrieve the statistics from the previous writer run. | verbose=False |
| get_statistics_writer | Retrieve the statistics from a running writer process. | verbose=False |
| get_status  | Return the status of the PCO writer client instance.  | verbose=False |
| get_status_last_run  | Retrieve the status of the previous writer process. | |
| get_status_writer | Retrieve the status of the writer server. | |
| get_written_frames | Return the number of frames written to file. | |
| is_connected | Verify whether a connection to the writer service is available. |  |
| is_running | Verify wether a writer process is currently running. |  |
| kill | Kill the currently running writer process. | verbose=False |
| reset | Reset the writer client object. |  |
| start | Start a new writer process. | wait=True, timeout=10, verbose=False |
| stop | Stop the writer process. | wait=True, timeout=10,verbose=False |
| validate_configuration | Validate that the current configuration parameters are valid and sufficient for an acquisition. | |
| wait | Wait for the writer to finish the writing process. |  verbose=False |
| wait_nframes |Wait for the writer to have written a given number of frames to file. |  nframes, inactivity_timeout=-1, verbose=False|


