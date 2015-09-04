"""
RDS Plugin

"""
from boto.ec2.cloudwatch import CloudWatchConnection
import datetime
import time
import logging


from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)

class RDS(base.Plugin):


    metrics = {"CPUUtilization":{"type":"float", "value":None, "unit":"percent"},
        "ReadLatency":{"type":"float", "value":None, "unit":"ms"},
        "DatabaseConnections":{"type":"int", "value":None, "unit":"count"},
        "FreeableMemory":{"type":"float", "value":None, "unit":"mb"},
        "ReadIOPS":{"type":"int", "value":None, "unit":"iops"},
        "WriteLatency":{"type":"float", "value":None, "unit":"ms"},
        "WriteThroughput":{"type":"float", "value":None, "unit":"mb/second"},
        "WriteIOPS":{"type":"int", "value":None, "unit":"iops"},
        "SwapUsage":{"type":"float", "value":None, "unit":"mb"},
        "ReadThroughput":{"type":"float", "value":None, "unit":"mb/second"},
        "FreeStorageSpace":{"type":"float", "value":None, "unit":"bytes"}}

    GUID = 'com.fivestars.DBmonitor'

    cur_time = {}
    last_time = {}
    last_space = {}
    days_left = {}

    def connect(self):
        region = self.config['region']
        access_key = self.config['access_key']
        secret_key = self.config['secret_key']

        conn = CloudWatchConnection(access_key, secret_key)
        return conn

    def poll(self):
        LOGGER.info("RDS poll method called")
        self.initialize()
        try:
            self.connection = self.connect()
        except:
            LOGGER.critical('COULD NOT CONNECT TO CLOUDWATCH RDS')
            return
        end = datetime.datetime.now()
        start = end - datetime.timedelta(minutes=1440)
        for k,vh in self.metrics.items():
            try:
                res = self.connection.get_metric_statistics(86400, start, end, k, "AWS/RDS", "Average", {"DBInstanceIdentifier": self.config['dbname']})
            except Exception, e:
                print "status err Error running rds_stats: %s" % e.error_message
                sys.exit(1)
            average = res[-1]["Average"] # last item in result set
            # if (k == "FreeStorageSpace" or k == "FreeableMemory"): #converts to gigs
                # average = average / 1024.0**3.0
            if vh["type"] == "float":
                self.metrics[k]["value"] = "%.4f" % average
            if vh["type"] == "int":
                self.metrics[k]["value"] = "%i" % average
        for k,vh in self.metrics.items():
            cur_value = float(vh['value'])  
            if (k == "FreeStorageSpace" or k == "FreeableMemory"):
                if (k == "FreeStorageSpace"):    
                    self.add_gauge_value('Disk Utilization/%s' % k, vh['unit'], cur_value) 
                elif (k == "FreeableMemory"):
                    self.add_derive_value('Overview Change/RDS/%s' % k, vh['unit'], cur_value)
                self.add_derive_value('Overview Change/RDS/%s' % k, vh['unit'], cur_value)

                RDS.cur_time[k] = time.time()
                if k in RDS.last_time:
                    elapsed_time = RDS.cur_time[k] - RDS.last_time[k] #
                    space_diff = RDS.last_space[k]-cur_value
                    if space_diff > 0:
                        seconds_left = ((cur_value*1.0)/space_diff)*elapsed_time
                        RDS.days_left[k] = seconds_left/86400.0
                        self.add_gauge_value('Disk Utilization/%sTrajectory' % k, 'days left', RDS.days_left[k])
                    elif k in RDS.days_left:
                        self.add_gauge_value('Disk Utilization/%sTrajectory' % k, 'days left', RDS.days_left[k])
                RDS.last_space[k] = cur_value
                RDS.last_time[k] = RDS.cur_time[k]
            else:
                self.add_gauge_value('Overview/RDS/%s' % k, vh['unit'], cur_value)
                self.add_derive_value('Overview Change/RDS/%s' % k, vh['unit'], cur_value)
        self.finish()

        
