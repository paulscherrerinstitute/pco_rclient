import multiprocessing

from pco_run import pco_test


ioc_name_1 = 'X02DA-CCDCAM1'
cam_name_1 = 'pco1'
connection_address_1 = 'tcp://10.10.1.26:8080'
flask_api_address_1  = 'http://xbl-daq-34:9901'
writer_api_address_1 = 'http://xbl-daq-34:9555'

connection_address_2 = 'tcp://10.10.1.202:8080'
flask_api_address_2  = 'http://xbl-daq-34:9902'
writer_api_address_2 = 'http://xbl-daq-34:9556'
ioc_name_2 = 'X02DA-CCDCAM2'
cam_name_2 = 'pco2'

process_list = [multiprocessing.Process(target=pco_test, args=(ioc_name_1,
                                cam_name_1))]
process_list.append(
    multiprocessing.Process(target=pco_test, 
                            args=(ioc_name_2,
                                cam_name_2))
)

for process in process_list:
    process.start()

for process in process_list:
    process.join()
