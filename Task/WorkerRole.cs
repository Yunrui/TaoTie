using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Diagnostics.Management;

namespace Task
{
    public class WorkerRole : RoleEntryPoint
    {
        public override void Run()
        {
            // This is a sample worker implementation. Replace with your logic.
            Trace.TraceInformation("Task entry point called");
            Service service = new Service();
            while (true)
            {
                Thread.Sleep(10000);

                // Next round Service Maintain
                service.Run();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 12;

            // In Cloud, we are going to put all App log into Table WADLogsTable
            if (!RoleEnvironment.IsEmulated)
            {
                var config = DiagnosticMonitor.GetDefaultInitialConfiguration();
                config.Logs.ScheduledTransferPeriod = System.TimeSpan.FromMinutes(1.0);
                config.Logs.ScheduledTransferLogLevelFilter = LogLevel.Information;

                DiagnosticMonitor.Start("Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString", config);
            }
            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            return base.OnStart();
        }
    }
}
