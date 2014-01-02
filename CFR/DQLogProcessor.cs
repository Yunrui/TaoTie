using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CFR
{
    /// <summary>
    /// DQLogProcessor is used to parse DQLog and calacuate per request completness
    /// </summary>
    class DQLogProcessor
    {
        public DQLogProcessor(string DQLog)
        {
            this.DQSets = this.ParseDQLog(DQLog);
        }

        internal Dictionary<string, string> DQSets { get; private set; }

        internal string ReportName 
        { 
            get 
            {
                string reportName = null;
                this.DQSets.TryGetValue("ReportName",out reportName);
                return reportName;
            } 
        }

        internal string TenantName
        {
            get
            {
                string tenantName = null;
                this.DQSets.TryGetValue("TenantName",out tenantName);
                return tenantName;
            }
        }

        private DateTime StartDate
        {
            get
            {
                string startDateStr = null;
                DateTime startDate = new DateTime(1,1,1);
                this.DQSets.TryGetValue("StartDate", out startDateStr);
                if (startDateStr != null)
                {
                    DateTime.TryParse(startDateStr, out startDate);
                }
                return startDate;
            }
        }

        private DateTime EndDate
        {
            get
            {
                string endDateStr = null;
                DateTime endDate = new DateTime(1, 1, 1);
                this.DQSets.TryGetValue("EndDate", out endDateStr);
                if (endDateStr != null)
                {
                    DateTime.TryParse(endDateStr, out endDate);
                }
                return endDate;
            }
        }

        private int Top
        {
            get
            {
                string top = null;
                this.DQSets.TryGetValue("Top", out top);
                if(top != null)
                {
                    return int.Parse(top);
                }
                return 0;
            }
        }

        private int XAxisPoints
        {
            get
            {
                string xAxisPointsStr = null;
                this.DQSets.TryGetValue("XAxisCount", out xAxisPointsStr);
                int xAxisPoints = 0;
                if (xAxisPoints != null)
                {
                    xAxisPoints = int.Parse(xAxisPointsStr);
                }
                return xAxisPoints;
            }
        }

        private int YAxisPoints
        {
            get
            {
                string yAxisPointsStr = null;
                this.DQSets.TryGetValue("YAxisCount", out yAxisPointsStr);
                int yAxisPoints = 0;
                if (yAxisPoints != null)
                {
                    yAxisPoints = int.Parse(yAxisPointsStr);
                }
                return yAxisPoints;
            }
        }

        private int PointsCount
        {
            get
            {
                string pointsStr = null;
                this.DQSets.TryGetValue("PointsCount", out pointsStr);
                int pointsCount = 0;
                if (pointsStr != null)
                {
                    pointsCount = int.Parse(pointsStr);
                }
                return pointsCount;
            }
        }

        internal double GetCompletness(DateTime requesttime)
        {
            int daysRange = GetDaysRange(requesttime, this.ReportName, this.StartDate, this.EndDate, this.Top);
            return CalcuateRequestCompletness(this.ReportName, this.XAxisPoints, this.YAxisPoints, daysRange, this.PointsCount);
        }

        /// <summary>
        /// Parse DQ log to dictionary
        /// ReportName:MailTraffic, TenantGuid:f69e53bf-b664-4172-8618-0d8e7f8129fc, TenantName:shinko.onmicrosoft.com, StartDate:2013-11-19 00:00:00, EndDate:2013-11-25 00:03:31, ReportType:Summary, ExpectRowsCountDaily:1, XAxisCount:7, YAxisCount:5, PointsCount:35
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        private Dictionary<string, string> ParseDQLog(string message)
        {
            Dictionary<string, string> set = new Dictionary<string, string>();

            string[] keyValuepairs = message.Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);

            for (int i = 0; i < keyValuepairs.Length; i++)
            {
                int position = keyValuepairs[i].IndexOf(':');

                string key = keyValuepairs[i].Substring(0, position);
                string value = keyValuepairs[i].Substring(position + 1);
                
                set.Add(key, value);
            }

            return set;
        }

        /// <summary>
        /// Calcuate the query days range
        /// </summary>
        /// <param name="requesttime">Request DateTime</param>
        /// <param name="reportName">Report Name</param>
        /// <param name="startDate">query start date</param>
        /// <param name="endDate">query end date</param>
        /// <param name="top">query top N</param>
        /// <returns>days range</returns>
        private int GetDaysRange(DateTime requesttime, string reportName, DateTime startDate, DateTime endDate, int top)
        {
            int daysRange = 0;
            DateTime retentionDate;
            if (requesttime.Hour <= 8)
            {
                retentionDate = requesttime.Date.AddDays(-2 - 90);
            }
            else
            {
                retentionDate = requesttime.Date.AddDays(-1 - 90);
            }

            if (retentionDate > startDate)
            {
                startDate = retentionDate;
            }

            if (endDate.Year < 10)
            {
                if (requesttime.Hour <= 8)
                {
                    endDate = requesttime.Date.AddDays(-2);
                }
                else
                {
                    endDate = requesttime.Date.AddDays(-1);
                }
            }

            if (top <= 0)
            {
                if (startDate.Year < 10)
                {
                    throw new Exception("top and startDate are both invalid!");
                }
                else
                {
                    if (reportName.IndexOf("Weekly", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        startDate = startDate.AddDays(-1 - ((int)startDate.DayOfWeek) + 7);
                        endDate = endDate.AddDays(-1 - ((int)endDate.DayOfWeek) + 7);
                        TimeSpan duration = endDate - startDate;
                        daysRange = (duration.Days + 1) / 7;
                        daysRange += 1;
                    }
                    else if (reportName.IndexOf("Monthly", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        if (startDate.Year == endDate.Year)
                        {
                            daysRange = endDate.Month - startDate.Month + 1;
                        }
                        else
                        {
                            daysRange = endDate.Month + 12 - startDate.Month + 1;
                        }
                    }
                    else if (reportName.IndexOf("Yearly", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        //daysRange = endDate.Year - startDate.Year + 1;
                        daysRange = 1;
                    }
                    else
                    {
                        TimeSpan duration = endDate - startDate;
                        daysRange = duration.Days + 1;
                    }
                }
            }
            else
            {
                if (startDate.Year < 10)
                {
                    daysRange = top;
                }
                else
                {
                    if (reportName.IndexOf("Weekly", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        startDate = startDate.AddDays(-1 - ((int)startDate.DayOfWeek) + 7);
                        endDate = endDate.AddDays(-1 - ((int)endDate.DayOfWeek) + 7);
                        TimeSpan duration = endDate - startDate;
                        daysRange = (duration.Days + 1) / 7;
                        daysRange += 1;
                    }
                    else if (reportName.IndexOf("Monthly", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        if (startDate.Year == endDate.Year)
                        {
                            daysRange = endDate.Month - startDate.Month + 1;
                        }
                        else
                        {
                            daysRange = endDate.Month + 12 - startDate.Month + 1;
                        }
                    }
                    else if (reportName.IndexOf("Yearly", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        daysRange = endDate.Year - startDate.Year + 1;
                    }
                    else
                    {
                        TimeSpan duration = endDate - startDate;
                        daysRange = duration.Days + 1;
                    }

                    daysRange = daysRange <= top ? daysRange : top;
                }
            }

            if (daysRange < 0)
            {
                daysRange = 0;
            }

            return daysRange;
        }

        /// <summary>
        /// Calcuate data completeness per request
        /// </summary>
        /// <param name="reportName">report name</param>
        /// <param name="xdataAxisCount">xdata points count</param>
        /// <param name="ydataAxisCount">ydata points count</param>
        /// <param name="daysRange">days range, min(enddate-dtartdate,top)</param>
        /// <param name="pointsCount">actual points count</param>
        /// <returns>data completness pct</returns>
        private double CalcuateRequestCompletness(string reportName, int xdataAxisCount, int ydataAxisCount, int daysRange, int pointsCount)
        {
            int expectRowsCount = 0;
            if (string.Compare(reportName, "StaleMailbox", StringComparison.OrdinalIgnoreCase) == 0)
            {
                expectRowsCount = daysRange * xdataAxisCount;
            }
            else
            {
                expectRowsCount = daysRange * ydataAxisCount;
            }

            double completness = 0.00;
            if (expectRowsCount != 0)
            {
                completness = (double)pointsCount / expectRowsCount;
            }
            else if (daysRange == 0)
            {
                completness = 1;
            }

            return completness >= 1 ? 1.00 : completness;
        }
    }
}
