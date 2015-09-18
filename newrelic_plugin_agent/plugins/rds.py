"""
RDS Plugin
by david.lee@fivestars.com
==============================
Uses boto.ec2.cloudwatch to get cloudwatch statistics from our RDS db instances
Sends both minute statistics (for basic RDS statistics) and weekly averages (for trajectory calculations)

"""
import boto.rds2
from boto.ec2.cloudwatch import CloudWatchConnection
import pytz, datetime, time
import logging
import sys


from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)

class RDS(base.Plugin):


    metrics = {"CPUUtilization":{"type":"float", "value":None, "unit":"percent"},
        "ReadLatency":{"type":"float", "value":None, "unit":"ms"},
        "DatabaseConnections":{"type":"int", "value":None, "unit":"count"},
        "FreeableMemory":{"type":"float", "value":None, "unit":"bytes"},
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


    def connect_cloudwatch(self):
        access_key = self.config['access_key']
        secret_key = self.config['secret_key']
        LOGGER.info('connecting to cloudwatch server')
        conn = CloudWatchConnection(access_key, secret_key)
        return conn

    def connect_rds(self):
        region = self.config['region']
        access_key = self.config['access_key']
        secret_key = self.config['secret_key']
        LOGGER.info('connecting to rds server')
        conn = boto.rds2.connect_to_region(region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        return conn

    def poll(self):
        LOGGER.info("RDS poll method called")
        self.initialize()
        try:
            self.connection_cloudwatch = self.connect_cloudwatch()
        except:
            LOGGER.info('CRITICAL: COULD NOT CONNECT TO CLOUDWATCH CLOUDWATCH')
            return
        LOGGER.info('connecting to cloudwatch server: success')
        #minute statistics
        end = datetime.datetime.now()
        start = end - datetime.timedelta(minutes=5)
        for k,vh in self.metrics.items():
            try:
                res = self.connection_cloudwatch.get_metric_statistics(60, start, end, k, "AWS/RDS", "Average", {"DBInstanceIdentifier": self.config['dbname']}) #boto needs a good amt of leeway to get responses from amazon (5 to 1 is my estimate)
            except Exception, e:
                LOGGER.info("RDS Plugin Connection Error")
                LOGGER.info(e.error_message)
                self.add_gauge_value('Plugin Error/RDS', count, 1) 
            else:
                latest_datetime = datetime.datetime.now() - datetime.timedelta(days=30)
                latest_data_pt = None
                for data_pt in res:
                    cur_datetime = data_pt['Timestamp']
                    if latest_datetime < cur_datetime:
                        latest_data_pt = data_pt
                        latest_datetime = cur_datetime
                average = latest_data_pt["Average"] # last item in result set
                if vh["type"] == "float":
                    self.metrics[k]["value"] = "%.4f" % average
                if vh["type"] == "int":
                    self.metrics[k]["value"] = "%i" % average

        for k,vh in self.metrics.items():
            cur_value = float(vh['value'])  
            if cur_value is not None:
                if (k == "FreeStorageSpace"):
                    self.add_gauge_value('Disk Utilization/%s' % k, vh['unit'], cur_value) 
                    self.add_derive_value('Overview Change/RDS/%s' % k, vh['unit'], cur_value)
                else:
                    self.add_gauge_value('Overview/RDS/%s' % k, vh['unit'], cur_value)
                    self.add_derive_value('Overview Change/RDS/%s' % k, vh['unit'], cur_value)

        #weekly statistics
        for k in ["FreeableMemory", "FreeStorageSpace"]:
            dbname = self.config['name']
            end = datetime.datetime.now()
            daily_start = end- datetime.timedelta(minutes=1440)
            try:
                res_weekly = self.connection_cloudwatch.get_metric_statistics(86400, daily_start, end, k, "AWS/RDS", "Average", {"DBInstanceIdentifier": dbname})
            except Exception, e:
                LOGGER.info('CRITICAL: RDS Plugin Connection Error')
                LOGGER.info(e.error_message)
                self.add_gauge_value('Plugin Error/RDS', count, 1)
            else:
                if len(res_weekly) > 0:
                    print "res_weekly > 0"
                    cur_value = float(res_weekly[-1]["Average"])
                    if dbname not in RDS.cur_time:
                        RDS.cur_time[dbname] = {}
                        RDS.last_time[dbname] = {}
                        RDS.last_space[dbname] = {}
                        RDS.days_left[dbname] = {}
                    RDS.cur_time[dbname][k] = time.time()
                    if k in RDS.last_time[dbname]:
                        print "k in RDS.last_time"
                        elapsed_time = RDS.cur_time[dbname][k] - RDS.last_time[dbname][k] #
                        space_diff = RDS.last_space[dbname][k]-cur_value
                        if space_diff > 0:
                            seconds_left = ((cur_value*1.0)/space_diff)*elapsed_time
                            RDS.days_left[dbname][k] = seconds_left/86400.0
                            self.add_gauge_value('Disk Utilization/%sTrajectory' % k, 'days left', RDS.days_left[dbname][k])
                        elif k in RDS.days_left[dbname]:
                            self.add_gauge_value('Disk Utilization/%sTrajectory' % k, 'days left', RDS.days_left[dbname][k])

                    RDS.last_space[dbname][k] = cur_value
                    RDS.last_time[dbname][k] = RDS.cur_time[dbname][k]



        #requires RDS metrics
        try:
            self.connection_rds = self.connect_rds()
        except Exception, e:
            LOGGER.info('CRITICAL: COULD NOT CONNECT TO CLOUDWATCH RDS: %s' %e)
            self.add_gauge_value('Plugin Error/RDS', count, 1)
        LOGGER.info('connecting to rds server: success')
        try:
            res = self.connection_rds.describe_db_instances(db_instance_identifier=self.config['dbname'])
        except Exception, e:
            LOGGER.info('CRITICAL: RDS Plugin Connection Error')
            print "status err Error running rds_stats: %s" % e
            self.add_gauge_value('Plugin Error/RDS', count, 1)
        else:
            total_space = float(res["DescribeDBInstancesResponse"]["DescribeDBInstancesResult"]["DBInstances"][0]["AllocatedStorage"]*1073741824)
            free_space = float(self.metrics["FreeStorageSpace"]["value"])
            if free_space is not None:
                used_space = total_space-free_space
                percent_space_used = int((used_space/total_space)*100)
                self.add_gauge_value('Disk Utilization/StorageSpaceUsedPercent', 'percent', percent_space_used)

        self.finish()

        
