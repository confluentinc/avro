/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.Globalization;
using Avro.Util;
using NUnit.Framework;

namespace Avro.Test
{
    [TestFixture]
    class LogicalTypeTests
    {
        [TestCase("01/01/2019")]
        [TestCase("05/05/2019")]
        [TestCase("05/05/2019 00:00:00Z")]
        [TestCase("05/05/2019 01:00:00Z")]
        [TestCase("05/05/2019 01:00:00")]
        public void TestDate(string s)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"int\", \"logicalType\": \"date\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);
            date = TimeZoneInfo.ConvertTime(date, TimeZoneInfo.FindSystemTimeZoneById("GB-Eire"));

            var avroDate = new Date();

            var convertedDate = (DateTime)avroDate.ConvertToLogicalValue(avroDate.ConvertToBaseValue(date, schema), schema);

            Assert.AreEqual(new TimeSpan(0, 0, 0), convertedDate.TimeOfDay); // the time should always be 00:00:00
            Assert.AreEqual(date.Date, convertedDate.Date);
        }

        [TestCase("01/01/2019 14:20:00Z", "01/01/2019 14:20:00Z")]
        [TestCase("01/01/2019 14:20:00", "01/01/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00Z", "05/05/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00", "05/05/2019 13:20:00Z")]
        [TestCase("05/05/2019 00:00:00Z", "05/05/2019 00:00:00Z")]
        [TestCase("05/05/2019 00:00:00", "05/04/2019 23:00:00Z")] // adjusted to UTC
        public void TestTimestamp(string s, string e)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"int\", \"logicalType\": \"date\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);
            date = TimeZoneInfo.ConvertTime(date, TimeZoneInfo.FindSystemTimeZoneById("GB-Eire"));

            var expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            var avroTimestampMilli = new TimestampMillisecond();
            var convertedDate = (DateTime)avroTimestampMilli.ConvertToLogicalValue(avroTimestampMilli.ConvertToBaseValue(date, schema), schema);
            Assert.AreEqual(expectedDate, convertedDate);

            var avroTimestampMicro = new TimestampMillisecond();
            convertedDate = (DateTime)avroTimestampMicro.ConvertToLogicalValue(avroTimestampMicro.ConvertToBaseValue(date, schema), schema);
            Assert.AreEqual(expectedDate, convertedDate);
        }

        [TestCase("01:20:10", "01:20:10", false)]
        [TestCase("23:00:00", "23:00:00", false)]
        [TestCase("01:00:00:00", null, true)]
        public void TestTime(string s, string e, bool expectRangeError)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"int\", \"logicalType\": \"time-millis\"}");

            var time = TimeSpan.Parse(s);
            
            var avroTimeMilli = new TimeMillisecond();
            var avroTimeMicro = new TimeMicrosecond();

            if (expectRangeError)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                {
                    avroTimeMilli.ConvertToLogicalValue(avroTimeMilli.ConvertToBaseValue(time, schema), schema);
                });
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                {
                    avroTimeMicro.ConvertToLogicalValue(avroTimeMilli.ConvertToBaseValue(time, schema), schema);
                });
            }
            else
            {
                var expectedTime = TimeSpan.Parse(e);

                var convertedTime = (TimeSpan)avroTimeMilli.ConvertToLogicalValue(avroTimeMilli.ConvertToBaseValue(time, schema), schema);
                Assert.AreEqual(expectedTime, convertedTime);

                convertedTime = (TimeSpan)avroTimeMicro.ConvertToLogicalValue(avroTimeMicro.ConvertToBaseValue(time, schema), schema);
                Assert.AreEqual(expectedTime, convertedTime);

            }
        }
    }
}
