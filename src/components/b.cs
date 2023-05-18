using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Activation;
using System.ServiceModel.Web;
using System.Text;
using DataMrDll;

[ServiceContract(Namespace = "")]
[AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Allowed)]
public class DataInfo
{
    // 要使用 HTTP GET，请添加 [WebGet] 特性。(默认 ResponseFormat 为 WebMessageFormat.Json)
    // 要创建返回 XML 的操作，
    //     请添加 [WebGet(ResponseFormat=WebMessageFormat.Xml)]，
    //     并在操作正文中包括以下行:
    //         WebOperationContext.Current.OutgoingResponse.ContentType = "text/xml";
    [OperationContract]
    public void DoWork()
    {
        // 在此处添加操作实现
        return;
    }
    [OperationContract]
    public static string Key_encryption(string tablename)
    {
        string str = keyDll.KeyClass.set_Key_String("MMIN", tablename);
        return str;
    }

    // 查询
    public static string QueryTableBySQL_test(string sqlText, string key)
    {
        try
        {
            key = keyDll.KeyClass.get_Key("MMKJ", key).ToLower();

            if (key.IndexOf("select") > -1 || key.IndexOf("from") > -1 || key.IndexOf("where") > -1)
            { return "Key值不正确，请联系管理员"; }
            if (key.Length < 6)
            { return "Key值不正确，请联系管理员"; }
            if (sqlText.ToLower().IndexOf(key.ToLower()) < 3)
            { return "Key值不正确，请联系管理员"; }

            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;
            return OracleAccess.DataTableToJson(dataTable);
        }
        catch (Exception ex)
        {
            return ex.Message + ex.TargetSite;
        }
    }
    //修改
    public static int UpdateTableBySQL_test(string sqlText, string key)
    {

        int rowsAffected = 0;
        try
        {
            key = keyDll.KeyClass.get_Key("MMKJ", key).ToLower();
            if (key.IndexOf("select") > -1 || key.IndexOf("from") > -1 || key.IndexOf("where") > -1 || key.IndexOf("update") > -1 || key.IndexOf("insert") > -1 || key.IndexOf("delete") > -1 || key.IndexOf("value") > -1)
            { return 0; };
            if (sqlText.ToLower().IndexOf("drop") > -1 || sqlText.ToLower().IndexOf("truncate") > -1 || sqlText.ToLower().IndexOf("alter") > -1)
            { return 0; };
            if (key.Length < 6)
            { return 0; }
            if (sqlText.ToLower().IndexOf(key.ToLower()) < 3)
            { return 0; }

            string Ex = "";
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            rowsAffected = oracleAccess.UpdateTableBySQL(sqlText, out Ex);
        }
        catch (System.Exception ex)
        {
        }
        return rowsAffected;
    }
    [OperationContract]
    public static string QueryTableBySQL(string sqlText, string key)
    {
        try
        {
            key = keyDll.KeyClass.get_Key("MMKJ", key).ToLower();

            if (key.IndexOf("select") > -1 || key.IndexOf("from") > -1 || key.IndexOf("where") > -1)
            { return "Key值不正确，请联系管理员"; }
            if (key.Length < 6)
            { return "Key值不正确，请联系管理员"; }
            if (sqlText.ToLower().IndexOf(key.ToLower()) < 3)
            { return "Key值不正确，请联系管理员"; }

            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;
            return OracleAccess.DataTableToJson(dataTable);
        }
        catch (Exception ex)
        {
            return ex.Message + ex.TargetSite;
        }
    }

    public static DataTable QueryTableBySQL_t(string sqlText)
    {
        try
        {
            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return null;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return null;
            return dataTable;
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    [OperationContract]
    public string get_key()
    {
        return OracleAccess.get_key();
    }
    [OperationContract]
    public int UpdateTableBySQL(string sqlText)
    {
        int rowsAffected = 0;
        try
        {
            string Ex = "";
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            rowsAffected = oracleAccess.UpdateTableBySQL(sqlText, out Ex);
        }
        catch (System.Exception ex)
        {
        }
        return rowsAffected;
    }

    [OperationContract]
    public string get_bg_line(string featid)
    {
        string sql = "select featid from cz_dtxfb where ycjzh ='" + featid + "'";
        DataTable dt = QueryTableBySQL_t(sql);
        if (dt.Rows.Count == 0) return "";
        featid = dt.Rows[0][0].ToString();
        sql = "select point1,point2,line from DT_RELINFO where (point1 ='" + featid + "' or point2 ='" + featid + "') and line is not null";
        dt = QueryTableBySQL_t(sql);
        List<string> list_bj = new List<string>();
        //list_bj.Add(dt.Rows[0]["LINE"].ToString());
        bool isgl = false;
        for (int i = 0; i < dt.Rows.Count; i++)
        {
            if (dt.Rows[i]["POINT1"].ToString() == featid)
            {
                sql = "select * from CZ_ALARMLOG t where oid='" + dt.Rows[i]["POINT2"].ToString() + "' and ALARMtype='压力预警'";
                DataTable dt1 = QueryTableBySQL_t(sql);
                if (dt1.Rows.Count > 0)
                {
                    isgl = true;
                    list_bj.Add(dt.Rows[i]["LINE"].ToString());
                }
            }
            if (dt.Rows[i]["POINT2"].ToString() == featid)
            {
                sql = "select * from CZ_ALARMLOG t where oid='" + dt.Rows[i]["POINT1"].ToString() + "' and ALARMtype='压力预警'";
                DataTable dt1 = QueryTableBySQL_t(sql);
                if (dt1.Rows.Count > 0)
                {
                    isgl = true;
                    list_bj.Add(dt.Rows[i]["LINE"].ToString());
                }
            }
        }
        if (isgl)
        {
            string li = "";
            for (int i = 0; i < list_bj.Count; i++)
            {
                li = li + list_bj[i] + "$";
            }
            return li;
        }
        sql = "select upl,dl from DT_POINT_L where featid ='" + featid + "' ";
        dt = QueryTableBySQL_t(sql);
        if (dt.Rows.Count > 0)
        { return dt.Rows[0]["UPL"].ToString() + "$" + dt.Rows[0]["DL"].ToString(); }
        return "";
    }
    [OperationContract]
    public string QueryTableBySQL_Q(string sqlText)
    {
        string Ex = string.Empty;
        OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
        System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

        if (dataSet == null) return string.Empty;
        if (dataSet.Tables.Count == 0) return string.Empty;
        DataTable dataTable = dataSet.Tables[0];
        if (dataTable == null) return string.Empty;
        return OracleAccess.DataTableToJson_TB(dataTable);
    }

    public string QueryTableBySQL_D(string sqlText)
    {
        string Ex = string.Empty;
        OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
        System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

        if (dataSet == null) return string.Empty;
        if (dataSet.Tables.Count == 0) return string.Empty;
        DataTable dataTable = dataSet.Tables[0];
        if (dataTable == null) return string.Empty;
        return OracleAccess.DataTableToJson_D(dataTable);
    }
    [OperationContract]
    public string Read_FlowMerter(string ID, string starTime, string endTime)
    {
        //DateTime newtime = Convert.ToDateTime(endTime);
        string sqltxt = "select * from FLOWTIMEDATA t WHERE ROWNUM <500";
        sqltxt = "select t.recordtime,t.RECORDVALUE from FLOW_DATA_" + ID + " t where "
            + " t.recordtime >= to_date('" + starTime + "','yyyy-mm-dd  hh24:mi:ss') and t.recordtime < to_date('" + endTime + "','yyyy-mm-dd  hh24:mi:ss') order by recordtime desc";
        return QueryTableBySQL_D(sqltxt);
    }
    [OperationContract]
    public string Read_YLD(string ID, string starTime, string endTime)
    {
        //DateTime newtime = Convert.ToDateTime(endTime);
        string sqltxt = "select * from FLOWTIMEDATA t WHERE ROWNUM <500";
        sqltxt = "select t.recordtime,t.PRESSUREVALUE from FLOW_DATA_" + ID + " t where "
            + " t.recordtime >= to_date('" + starTime + "','yyyy-mm-dd  hh24:mi:ss') and t.recordtime < to_date('" + endTime + "','yyyy-mm-dd  hh24:mi:ss') order by recordtime desc";
        return QueryTableBySQL_D(sqltxt);
    }
    //[OperationContract]
    //public string Read_QW(string starTime, string endTime)
    //{
    //    //DateTime newtime = Convert.ToDateTime(endTime);
    //    string sqltxt = "select * from FLOWTIMEDATA t WHERE ROWNUM <500";
    //    sqltxt = "select t.INTIME,t.QWINFO from LKG_WEATHERINFO t where "
    //        + " t.INTIME >= to_date('" + starTime + "','yyyy-mm-dd  hh24:mi:ss') and t.INTIME < to_date('" + endTime + "','yyyy-mm-dd  hh24:mi:ss') order by INTIME asc";
    //    return QueryTableBySQL(sqltxt);
    //}

    public string get_jwToxy(string jd, string wd)
    {
        WebReference.DataService web = new WebReference.DataService();
        string ss = web.TransformGaoDeToCity(double.Parse(jd), double.Parse(wd), "changzhou");
        return ss;
    }

    public string get_jwToxy_bd(string jd, string wd)
    {
        WebReference.DataService web = new WebReference.DataService();
        string ss = web.TransformWGS84ToCity(double.Parse(jd), double.Parse(wd), "changzhou");
        return ss;
    }
    [OperationContract]
    public string get_point(string sqlText)
    {
        string sql = "select * from CZ_DTXFB t where t.NAME is not null and  t.NAME not like '%生活%'";
        DataTable dt = QueryTableBySQL_t(sql);
        for (int i = 0; i < dt.Rows.Count; i++)
        {
            string xy = get_xyTojw(dt.Rows[i]["Y"].ToString(), dt.Rows[i]["X"].ToString());
            dt.Rows[i]["Y"] = xy.Split('|')[0];
            dt.Rows[i]["X"] = xy.Split('|')[1];
        }
        return OracleAccess.DataTableToJson(dt);
    }

    [OperationContract]
    public string get_xyTojw(string x, string y)
    {
        WebReference.DataService web = new WebReference.DataService();
        string ss = web.TransformCityToWGS84(double.Parse(x), double.Parse(y), "changzhou");
        return ss;
    }

    [OperationContract]
    public string Read_RemoteMerter(string ID, string starTime, string endTime)
    {
        string[] ids = ID.Split(',');
        string result = "[";
        for (int i = 0; i < ids.Length; i++)
        {
            DateTime newtime = Convert.ToDateTime(endTime);
            string sqltxt = "select * from FLOWTIMEDATA t WHERE ROWNUM <500";
            sqltxt = "select s.recordtime,s.recordvalue from REMOTEMETERTIMEDATA S WHERE S.OBJECTID IN (select T.OBJECTID from REMOTEMETERINFO t WHERE T.OBJECTIDEX =" + ids[i]
                + ") and s.recordtime >= '" + starTime + "' and s.recordtime < '" + newtime.AddDays(1).ToString("yyyy-MM-dd") + "' order by s.recordtime desc";
            string sql_result = QueryTableBySQL_Q(sqltxt);
            if (sql_result == "") continue;
            result = result + sql_result.Substring(0, sql_result.Length - 1) + ",['设备号-" + ids[i] + "']],";
        }
        if (result == "[") return "";
        return result.Substring(0, result.Length - 1) + "]";
    }

    [OperationContract]
    public string get_Remote_value(string ID, string starTime, string endTime)
    {
        DateTime newtime = Convert.ToDateTime(endTime);
        string sqltxt = "select * from FLOWTIMEDATA t WHERE ROWNUM <500";
        sqltxt = "select s.recordtime,s.recordvalue,d.objectid,d.fullname from REMOTEMETERTIMEDATA S,REMOTEMETERINFO d WHERE s.objectid =d.objectid " +
            " and S.OBJECTID IN (select T.OBJECTID from REMOTEMETERINFO t WHERE T.OBJECTIDEX =" + ID
            + ") and s.recordtime >= '" + starTime + "' and s.recordtime < '" + newtime.AddDays(1).ToString("yyyy-MM-dd") + "' order by s.recordtime desc";
        return "";
    }

    /// <summary>
    /// 养护统计 空值录入0
    /// </summary>
    /// <param name="sqlText"></param>
    /// <returns></returns>
    [OperationContract]
    public string get_Maintenance(string sqlText)
    {
        try
        {
            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];

            DataTable table_new = dataTable;

            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int abc = table_new.Columns.Count;
                for (int k = 8; k < table_new.Columns.Count; k++)
                {
                    table_new.Columns[k].ReadOnly = false;
                    if (table_new.Rows[i][k].ToString() == "")
                    {
                        table_new.Rows[i][k] = "0";
                    }
                }
            }

            if (table_new == null) return string.Empty;
            return OracleAccess.DataTableToJson(table_new);
        }
        catch (Exception ex)
        {
            return ex.Message + ex.TargetSite;
        }
    }


    /// <summary>
    /// 抢修统计 空值录入0
    /// </summary>
    /// <param name="sqlText"></param>
    /// <returns></returns>
    [OperationContract]
    public string get_Patrol(string sqlText)
    {
        try
        {
            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];

            DataTable table_new = dataTable;

            //PIPELINELENGTH: { type: "number" },
            //                //抢修井室个数
            //                WELLNUMBER: { type: "number" },
            //                //抢修化粪池个数
            //                SEPTICTANKNUMBER: { type: "number" },
            //                //抢修隔油池个数
            //                GREASETRAPNUMBER: { type: "number" },
            //                //井盖更换个数
            //                WELLCOVERNUMBER: { type: "number" }

            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int abc = table_new.Columns.Count;
                for (int k = 4; k < table_new.Columns.Count; k++)
                {
                    if (table_new.Columns[k].ToString() == "PIPELINELENGTH" || table_new.Columns[k].ToString() == "WELLNUMBER" || table_new.Columns[k].ToString() == "SEPTICTANKNUMBER" || table_new.Columns[k].ToString() == "GREASETRAPNUMBER" || table_new.Columns[k].ToString() == "WELLCOVERNUMBER")
                    {
                        table_new.Columns[k].ReadOnly = false;
                        if (table_new.Rows[i][k].ToString() == "")
                        {
                            table_new.Rows[i][k] = "0";
                        }
                    }

                }
            }

            if (table_new == null) return string.Empty;
            return OracleAccess.DataTableToJson(table_new);
        }
        catch (Exception ex)
        {
            return ex.Message + ex.TargetSite;
        }
    }

    /// <summary>
    /// 每日养护
    /// </summary>
    /// <returns></returns>
    [OperationContract]
    public string get_Daily_maintenance(string starTime, string endTime, string Areadql, string gjz, string rwzl, string gjz_input, string UserID)
    {
        try
        {
            string sqlpqsx = "";
            string initialEndTime = endTime;
            sqlpqsx += Areadql.Trim();
            if (gjz.Trim() != "")
            {
                sqlpqsx += " and (t1.street like '%" + gjz.Trim() + "%' )";
            }
            if (rwzl.Trim() != "全部")
            {
                sqlpqsx += " and t1.SUBTYPE ='" + rwzl.Trim() + "' ";
            }

            sqlpqsx += " and a.AREAID in (select AREAID from sys_AREA_tree where PARENTAREAID  = (select AREAID from SYS_USERINFO where ID = '" + UserID.Trim() + "')) and t1.street like '%" + gjz_input.Trim() + "%' ";
            string sqlText = "  select  t1.id,a.areaid,a.areaname,t1.subtype,t1.street,s1.PIPELENGTHSUM,s1.WELLSUM,s1.createtime,t1.COMPANYID from YH_PIPELINE_TASK t1 left join YH_TASK_STATISTICS s1 on t1.id=s1.taskid inner join SYS_AREAS a on t1.areaid=a.areaid where t1.createtime >=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and t1.createtime <=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss') and DELFLAG=0 " + sqlpqsx;
            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;

            //判断时间段中一共有多少天
            System.TimeSpan t3 = DateTime.Parse(endTime) - DateTime.Parse(starTime);

            DataTable table_new = new DataTable();

            //添加列 

            //道路及小区名称
            DataColumn day_num = new DataColumn();
            day_num.ColumnName = "STREET";
            day_num.DataType = typeof(string);
            table_new.Columns.Add(day_num);
            //任务ID
            DataColumn day_num_ID = new DataColumn();
            day_num_ID.ColumnName = "TASKID";
            day_num_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID);

            //片区名称
            DataColumn day_num_taskdes = new DataColumn();
            day_num_taskdes.ColumnName = "AREANAME";
            day_num_taskdes.DataType = typeof(string);
            table_new.Columns.Add(day_num_taskdes);


            //任务子类
            DataColumn day_num1 = new DataColumn();
            day_num1.ColumnName = "SUBTYPE";
            day_num1.DataType = typeof(string);
            table_new.Columns.Add(day_num1);

            //length
            DataColumn day_num4 = new DataColumn();
            day_num4.ColumnName = "TXTLENGTH";
            day_num4.DataType = typeof(double);
            table_new.Columns.Add(day_num4);

            //管网总计
            DataColumn day_num5 = new DataColumn();
            day_num5.ColumnName = "PIPESUM";
            day_num5.DataType = typeof(double);
            table_new.Columns.Add(day_num5);

            //井室总计
            DataColumn day_num6 = new DataColumn();
            day_num6.ColumnName = "WELLSUM";
            day_num6.DataType = typeof(double);
            table_new.Columns.Add(day_num6);

            ////任务子类
            //DataColumn day_num2 = new DataColumn();
            //day_num2.ColumnName = "CREATETIME";
            //day_num2.DataType = typeof(string);
            //table_new.Columns.Add(day_num2);


            for (int i = 0; i < t3.TotalDays + 1; i++)
            {
                endTime = endTime.Replace("-", "/");
                string testdate = endTime;
                testdate = DateTime.Parse(endTime).ToString("日期yyyy年MM月dd日");
                DataColumn Ctime = new DataColumn();
                Ctime.ColumnName = testdate;
                Ctime.DataType = typeof(string);
                table_new.Columns.Add(Ctime);

                DataColumn CPipe = new DataColumn();
                CPipe.ColumnName = testdate + "管网";
                CPipe.DataType = typeof(string);
                table_new.Columns.Add(CPipe);

                DataColumn CWell = new DataColumn();
                CWell.ColumnName = testdate + "井室";
                CWell.DataType = typeof(string);
                table_new.Columns.Add(CWell);


                DateTime datetime1 = DateTime.Parse(endTime).AddDays(-1);
                endTime = datetime1.ToString("yyyy/MM/dd");

            }

            //LOGINID 序号 
            DataColumn day_num3 = new DataColumn();
            day_num3.ColumnName = "LOGINID";
            day_num3.DataType = typeof(int);
            table_new.Columns.Add(day_num3);


            int a1 = 1;
            int a2 = 0;
            for (int j = 0; j < dataTable.Rows.Count; j++)
            {
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["TASKID"].ToString() == dataTable.Rows[j]["ID"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {
                        string datetime2 = dataTable.Rows[j]["CREATETIME"].ToString();
                        //string aa = item.ItemArray[1].ToString();
                        //string bb = dataTable.Rows[j]["SIGNERNAME"].ToString();
                        //if (aa == bb)
                        //{

                        //}
                        a2 = item.ItemArray.Count() - 1;
                        int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = "管网：" + dataTable.Rows[j]["PIPELENGTHSUM"].ToString() + "米，井室：" + dataTable.Rows[j]["WELLSUM"].ToString() + "个";
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日") + "管网"] = dataTable.Rows[j]["PIPELENGTHSUM"].ToString();
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日") + "井室"] = dataTable.Rows[j]["WELLSUM"].ToString();
                    }
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    if (dataTable.Rows[j]["CREATETIME"].ToString() != "")
                    {
                        string datetime2 = dataTable.Rows[j]["CREATETIME"].ToString();
                        dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = "管网：" + dataTable.Rows[j]["PIPELENGTHSUM"].ToString() + "米；井室：" + dataTable.Rows[j]["WELLSUM"].ToString() + "个";
                        dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日") + "管网"] = dataTable.Rows[j]["PIPELENGTHSUM"].ToString();
                        dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日") + "井室"] = dataTable.Rows[j]["WELLSUM"].ToString();
                    }
                    dr["TASKID"] = dataTable.Rows[j]["ID"].ToString();
                    dr["STREET"] = dataTable.Rows[j]["STREET"].ToString();
                    dr["AREANAME"] = dataTable.Rows[j]["AREANAME"].ToString();
                    dr["SUBTYPE"] = dataTable.Rows[j]["SUBTYPE"].ToString();


                    dr["LOGINID"] = a1;
                    a1++;
                    table_new.Rows.Add(dr);
                }
            }


            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int abc = table_new.Columns.Count;
                double pipesum = 0;
                double wellsum = 0;
                for (int k = 0; k < table_new.Columns.Count; k++)
                {
                    if (table_new.Rows[i][k].ToString() == "" && table_new.Columns[k].ToString() != "TXTLENGTH")
                    {
                        table_new.Rows[i][k] = "0";
                    }
                    if (table_new.Columns[k].ToString() == "TXTLENGTH")
                    {

                        table_new.Rows[i]["TXTLENGTH"] = (table_new.Rows[i][DateTime.Parse(initialEndTime).ToString("日期yyyy年MM月dd日")]).ToString().Length;
                    }
                    if (table_new.Columns[k].ToString().IndexOf("管网") != -1)
                    {
                        if (table_new.Rows[i][k].ToString() != "")
                        {
                            pipesum += double.Parse(table_new.Rows[i][k].ToString());
                        }
                    }

                    if (table_new.Columns[k].ToString().IndexOf("井室") != -1)
                    {
                        if (table_new.Rows[i][k].ToString() != "")
                        {
                            wellsum += double.Parse(table_new.Rows[i][k].ToString());
                        }
                    }

                }
                table_new.Rows[i]["PIPESUM"] = pipesum;
                table_new.Rows[i]["WELLSUM"] = wellsum;
            }

            //
            //timesum += double.Parse(table_new.Rows[i][j].ToString());

            //return JsonConvert.SerializeObject(dataTable);
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }

    /// <summary>
    /// 签到信息
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Sign_in_info(string keyword, string starTime, string endTime, string Sign_s, string UserID, string sqlpqsx)
    {
        try
        {
            string sqlpqsx1 = "";
            sqlpqsx1 += " and decode(task.TYPECODE,null,t1.type,task.TYPECODE)='" + Sign_s + "' ";
            sqlpqsx1 += " and (t1.SIGNAREA in (select AREAID from sys_AREA_tree where PARENTAREAID  = (select AREAID from SYS_USERINFO where ID = '" + UserID + "')) or t1.SIGNAREA=7 ) ";
            string sqlText = "select to_char(t1.signintime,'yyyy-mm-dd') as signintime,t2.NAME as SIGNERNAME,t1.SIGNAREA,t1.SIGNCOMPANY,a.areaname,task.DELFLAG,decode(task.TYPECODE,null,t1.type,task.TYPECODE) as TYPECODE, decode(t3.STREET,null,t4.TASKID,t3.STREET) as TASKNAME,t4.taskdes,t1.TASKID,t1.SIGNER,COUNT(*) SIGNINNUM,sum(ceil((t1.SIGNOUTTIME-t1.SIGNINTIME)*24*60)) DURATION,COUNT(t5.TASKID) NOSIGN  from （ select * from YH_TASK_SIGN where SIGNINTIME>=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and SIGNINTIME<=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss') ) t1 left join ( select * from YH_TASK_SIGN where SIGNINTIME>=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and SIGNINTIME<=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss') and SIGNOUTTIME is null ) t5 on t1.TASKID=t5.TASKID and  (t1.SIGNINTIME=t5.SIGNINTIME ) left join YH_TASK task on t1.TASKID = task.TASKID left join SYS_USERINFO t2 on t1.SIGNER = t2.ID left join YH_PIPELINE_TASK t3 on t1.TASKID = t3.ID left join SYS_AREAS a on t1.SIGNAREA=a.AREAID  left join YH_PUMP_STATION_TASK t4 on  t1.TASKID=t4.taskid  group by to_char(t1.signintime,'yyyy-mm-dd'),t2.NAME , t1.SIGNAREA,a.areaname, decode(t3.STREET,null,t4.TASKID,t3.STREET),t1.TASKID,t1.SIGNER,t1.SIGNAREA,t1.SIGNCOMPANY,task.DELFLAG,t4.taskdes,decode(task.TYPECODE,null,t1.type,task.TYPECODE) having  (decode(t3.STREET,null,t4.TASKID,t3.STREET) like '%" + keyword + "%' or t2.NAME like '%" + keyword + "%' ) and (task.DELFLAG= 0 or task.DELFLAG is null )  and (t1.TASKID is null or t1.TASKID is not null  )  " + sqlpqsx1 + sqlpqsx + " ";

            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;

            //判断时间段中一共有多少天
            System.TimeSpan t3 = DateTime.Parse(endTime) - DateTime.Parse(starTime);

            DataTable table_new = new DataTable();

            //添加列 

            //任务名称
            DataColumn day_num = new DataColumn();
            day_num.ColumnName = "TASKNAME";
            day_num.DataType = typeof(string);
            table_new.Columns.Add(day_num);
            //任务ID
            DataColumn day_num_ID = new DataColumn();
            day_num_ID.ColumnName = "TASKID";
            day_num_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID);

            //任务描述
            DataColumn day_num_taskdes = new DataColumn();
            day_num_taskdes.ColumnName = "TASKDES";
            day_num_taskdes.DataType = typeof(string);
            table_new.Columns.Add(day_num_taskdes);


            //签到人
            DataColumn day_num1 = new DataColumn();
            day_num1.ColumnName = "SIGNERNAME";
            day_num1.DataType = typeof(string);
            table_new.Columns.Add(day_num1);

            //片区
            DataColumn day_numpq = new DataColumn();
            day_numpq.ColumnName = "SIGNAREA";
            day_numpq.DataType = typeof(string);
            table_new.Columns.Add(day_numpq);

            //片区名称
            DataColumn day_numpqID = new DataColumn();
            day_numpqID.ColumnName = "AREANAME";
            day_numpqID.DataType = typeof(string);
            table_new.Columns.Add(day_numpqID);

            //签到类型 TYPECODE
            DataColumn day_TYPECODE = new DataColumn();
            day_TYPECODE.ColumnName = "TYPECODE";
            day_TYPECODE.DataType = typeof(string);
            table_new.Columns.Add(day_TYPECODE);

            //签到人ID
            DataColumn day_num1_ID = new DataColumn();
            day_num1_ID.ColumnName = "SIGNER";
            day_num1_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num1_ID);

            //签到时长 DURATION
            DataColumn day_num1_DURATION = new DataColumn();
            day_num1_DURATION.ColumnName = "DURATION";
            day_num1_DURATION.DataType = typeof(double);
            table_new.Columns.Add(day_num1_DURATION);

            //签到天数
            DataColumn day_num4 = new DataColumn();
            day_num4.ColumnName = "SIGNINSUM";
            day_num4.DataType = typeof(int);
            table_new.Columns.Add(day_num4);





            for (int i = 0; i < t3.TotalDays + 1; i++)
            {
                endTime = endTime.Replace("-", "/");
                string testdate = endTime;
                testdate = DateTime.Parse(endTime).ToString("日期yyyy年MM月dd日");
                DataColumn Ctime = new DataColumn();
                Ctime.ColumnName = testdate;
                Ctime.DataType = typeof(string);
                table_new.Columns.Add(Ctime);
                DateTime datetime1 = DateTime.Parse(endTime).AddDays(-1);
                endTime = datetime1.ToString("yyyy/MM/dd");

            }

            //LOGINID 序号 
            DataColumn day_num3 = new DataColumn();
            day_num3.ColumnName = "LOGINID";
            day_num3.DataType = typeof(int);
            table_new.Columns.Add(day_num3);


            int a1 = 1;
            int a2 = 0;
            for (int j = 0; j < dataTable.Rows.Count; j++)
            {
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["TASKID"].ToString() == dataTable.Rows[j]["TASKID"].ToString() && a["SIGNER"].ToString() == dataTable.Rows[j]["SIGNER"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {
                        string datetime2 = dataTable.Rows[j]["SIGNINTIME"].ToString();
                        a2 = item.ItemArray.Count() - 1;
                        int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());
                        if (dataTable.Rows[j]["NOSIGN"].ToString() == "0")
                        {
                            if (dataTable.Rows[j]["DURATION"].ToString() == "")
                            {
                                table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["SIGNINNUM"].ToString() + "|" + "无签退";
                            }
                            else
                            {
                                table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["SIGNINNUM"].ToString();
                            }
                        }
                        else
                        {
                            table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["SIGNINNUM"].ToString() + "|" + "无签退";
                        }


                        if (table_new.Rows[cc - 1]["DURATION"].ToString().Trim() != "")
                        {
                            double timesum = double.Parse(table_new.Rows[cc - 1]["DURATION"].ToString());
                            if (dataTable.Rows[j]["DURATION"].ToString().Trim() != "")
                            {
                                table_new.Rows[cc - 1]["DURATION"] = timesum + double.Parse(dataTable.Rows[j]["DURATION"].ToString());
                            }
                            else
                            {
                                table_new.Rows[cc - 1]["DURATION"] = timesum + 0;
                            }

                        }

                        //dr["DURATION"] += dataTable.Rows[j]["DURATION"].ToString();
                    }
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    if (dataTable.Rows[j]["SIGNINTIME"].ToString() != "")
                    {
                        string datetime2 = dataTable.Rows[j]["SIGNINTIME"].ToString();
                        if (dataTable.Rows[j]["NOSIGN"].ToString() == "0")
                        {
                            if (dataTable.Rows[j]["DURATION"].ToString() != "")
                            {
                                dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["SIGNINNUM"].ToString();
                            }
                            else
                            {
                                dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["SIGNINNUM"].ToString() + "|" + "无签退";
                            }

                        }
                        else
                        {
                            dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["SIGNINNUM"].ToString() + "|" + "无签退";
                        }

                    }
                    dr["TASKID"] = dataTable.Rows[j]["TASKID"].ToString();
                    dr["SIGNER"] = dataTable.Rows[j]["SIGNER"].ToString();
                    dr["TASKNAME"] = dataTable.Rows[j]["TASKNAME"].ToString();
                    dr["SIGNERNAME"] = dataTable.Rows[j]["SIGNERNAME"].ToString();
                    dr["SIGNAREA"] = dataTable.Rows[j]["SIGNAREA"].ToString();
                    dr["AREANAME"] = dataTable.Rows[j]["AREANAME"].ToString();
                    dr["TYPECODE"] = dataTable.Rows[j]["TYPECODE"].ToString();
                    dr["TASKDES"] = dataTable.Rows[j]["TASKDES"].ToString();
                    if (dataTable.Rows[j]["DURATION"].ToString().Trim() == "")
                    {
                        dr["DURATION"] = 0;
                    }
                    else
                    {
                        dr["DURATION"] = double.Parse(dataTable.Rows[j]["DURATION"].ToString());
                    }

                    dr["LOGINID"] = a1;
                    a1++;
                    table_new.Rows.Add(dr);
                }
            }


            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int abc = table_new.Columns.Count;
                for (int k = 0; k < table_new.Columns.Count; k++)
                {
                    if (table_new.Rows[i][k].ToString() == "")
                    {
                        table_new.Rows[i][k] = "0";
                    }
                }
            }

            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int sum = 0;

                for (int j = 10; j < table_new.Columns.Count - 1; j++)
                {
                    if (table_new.Rows[i][j].ToString().IndexOf("|") != -1)
                    {
                        sum++;
                    }
                    else
                    {
                        if (int.Parse(table_new.Rows[i][j].ToString()) > 0)
                        {
                            sum++;
                        }
                    }

                }
                table_new.Rows[i]["SIGNINSUM"] = sum;
            }

            //
            //timesum += double.Parse(table_new.Rows[i][j].ToString());

            //return JsonConvert.SerializeObject(dataTable);
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }

    /// <summary>
    /// 签到信息 导出报表
    /// </summary>
    /// <param name="sqlText"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Sign_info_table(string sqlText, string starTime, string endTime)
    {
        string Ex = string.Empty;
        OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
        System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

        if (dataSet == null) return string.Empty;
        DataTable dataTable = dataSet.Tables[0];
        if (dataTable == null) return string.Empty;

        //判断时间段中一共有多少天
        System.TimeSpan t3 = DateTime.Parse(endTime) - DateTime.Parse(starTime);

        DataTable table_new = new DataTable();

        //添加列 

        //任务名称
        DataColumn day_num = new DataColumn();
        day_num.ColumnName = "TASKNAME";
        day_num.DataType = typeof(string);
        table_new.Columns.Add(day_num);
        //任务ID
        DataColumn day_num_ID = new DataColumn();
        day_num_ID.ColumnName = "TASKID";
        day_num_ID.DataType = typeof(string);
        table_new.Columns.Add(day_num_ID);

        //任务描述
        DataColumn day_num_taskdes = new DataColumn();
        day_num_taskdes.ColumnName = "TASKDES";
        day_num_taskdes.DataType = typeof(string);
        table_new.Columns.Add(day_num_taskdes);


        //签到人
        DataColumn day_num1 = new DataColumn();
        day_num1.ColumnName = "SIGNERNAME";
        day_num1.DataType = typeof(string);
        table_new.Columns.Add(day_num1);

        //片区
        DataColumn day_numpq = new DataColumn();
        day_numpq.ColumnName = "SIGNAREA";
        day_numpq.DataType = typeof(string);
        table_new.Columns.Add(day_numpq);

        //片区名称
        DataColumn day_numpqID = new DataColumn();
        day_numpqID.ColumnName = "AREANAME";
        day_numpqID.DataType = typeof(string);
        table_new.Columns.Add(day_numpqID);

        //签到类型 TYPECODE
        DataColumn day_TYPECODE = new DataColumn();
        day_TYPECODE.ColumnName = "TYPECODE";
        day_TYPECODE.DataType = typeof(string);
        table_new.Columns.Add(day_TYPECODE);

        //签到人ID
        DataColumn day_num1_ID = new DataColumn();
        day_num1_ID.ColumnName = "SIGNER";
        day_num1_ID.DataType = typeof(string);
        table_new.Columns.Add(day_num1_ID);

        //天气 weather
        DataColumn day_num1_DURATION = new DataColumn();
        day_num1_DURATION.ColumnName = "WEATHER";
        day_num1_DURATION.DataType = typeof(string);
        table_new.Columns.Add(day_num1_DURATION);

        ////签到时间
        //DataColumn day_num4 = new DataColumn();
        //day_num4.ColumnName = "SIGNINSUM";
        //day_num4.DataType = typeof(string);
        //table_new.Columns.Add(day_num4);



        for (int i = 0; i < t3.TotalDays + 1; i++)
        {
            starTime = starTime.Replace("-", "/");
            string testdate = starTime;
            testdate = DateTime.Parse(starTime).ToString("日期yyyy年MM月dd日");
            DataColumn Ctime = new DataColumn();
            Ctime.ColumnName = testdate;
            Ctime.DataType = typeof(string);
            table_new.Columns.Add(Ctime);
            DateTime datetime1 = DateTime.Parse(starTime).AddDays(1);
            starTime = datetime1.ToString("yyyy/MM/dd");

        }

        //LOGINID 序号 
        DataColumn day_num3 = new DataColumn();
        day_num3.ColumnName = "LOGINID";
        day_num3.DataType = typeof(int);
        table_new.Columns.Add(day_num3);

        DataRow dr2 = table_new.NewRow();
        table_new.Rows.Add(dr2);

        int a1 = 2;
        int a2 = 0;
        for (int j = 0; j < dataTable.Rows.Count; j++)
        {
            var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["TASKID"].ToString() == dataTable.Rows[j]["TASKID"].ToString() && a["SIGNER"].ToString() == dataTable.Rows[j]["SIGNER"].ToString());
            if (query1.Count() > 0)
            {
                foreach (var item in query1)
                {
                    string datetime2 = dataTable.Rows[j]["SIGNINTIME"].ToString();
                    //string aa = item.ItemArray[1].ToString();
                    //string bb = dataTable.Rows[j]["SIGNERNAME"].ToString();
                    //if (aa == bb)
                    //{

                    //}
                    a2 = item.ItemArray.Count() - 1;
                    int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());

                    int CountDur = 0;

                    if (dataTable.Rows[j]["DURATION"].ToString() != "")
                    {
                        CountDur = int.Parse(dataTable.Rows[j]["DURATION"].ToString());
                    }

                    if (table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString() != "")
                    {
                        if ((table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString()).Split('|')[1] != "")
                        {
                            CountDur += int.Parse((table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString()).Split('|')[1]);
                        }
                    }
                    //else
                    //{
                    //    if (dataTable.Rows[j]["DURATION"].ToString() != "")
                    //    {
                    //        CountDur += int.Parse(dataTable.Rows[j]["DURATION"].ToString());
                    //    }
                    //    else
                    //    {
                    //        CountDur += 0;
                    //    }
                    //}


                    string SigninTime = dataTable.Rows[j]["SIGNINTIME"].ToString() + "|" + CountDur + "|" + dataTable.Rows[j]["WEATHER"].ToString();
                    table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = SigninTime;

                    if (dataTable.Rows[j]["WEATHER"].ToString() != "")
                    {
                        table_new.Rows[0][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["WEATHER"].ToString();
                    }


                    //if (table_new.Rows[cc - 1]["DURATION"].ToString().Trim() != "")
                    //{
                    //    double timesum = double.Parse(table_new.Rows[cc - 1]["DURATION"].ToString());
                    //    if (dataTable.Rows[j]["DURATION"].ToString().Trim() != "")
                    //    {
                    //        table_new.Rows[cc - 1]["DURATION"] = timesum + double.Parse(dataTable.Rows[j]["DURATION"].ToString());
                    //    }
                    //    else
                    //    {
                    //        table_new.Rows[cc - 1]["DURATION"] = timesum + 0;
                    //    }

                    //}

                    //dr["DURATION"] += dataTable.Rows[j]["DURATION"].ToString();
                }
            }
            else
            {
                DataRow dr = table_new.NewRow();
                if (dataTable.Rows[j]["SIGNINTIME"].ToString() != "")
                {
                    string datetime2 = dataTable.Rows[j]["SIGNINTIME"].ToString();
                    string SigninTime = dataTable.Rows[j]["SIGNINTIME"].ToString() + "|" + dataTable.Rows[j]["DURATION"].ToString() + "|" + dataTable.Rows[j]["WEATHER"].ToString();
                    dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = SigninTime;

                    if (dataTable.Rows[j]["WEATHER"].ToString() != "")
                    {
                        table_new.Rows[0][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["WEATHER"].ToString();
                    }
                }

                dr["TASKID"] = dataTable.Rows[j]["TASKID"].ToString();
                dr["SIGNER"] = dataTable.Rows[j]["SIGNER"].ToString();
                dr["TASKNAME"] = dataTable.Rows[j]["TASKNAME"].ToString();
                dr["SIGNERNAME"] = dataTable.Rows[j]["SIGNERNAME"].ToString();
                dr["SIGNAREA"] = dataTable.Rows[j]["SIGNAREA"].ToString();
                dr["AREANAME"] = dataTable.Rows[j]["AREANAME"].ToString();
                dr["TYPECODE"] = dataTable.Rows[j]["TYPECODE"].ToString();
                dr["TASKDES"] = dataTable.Rows[j]["TASKDES"].ToString();
                dr["WEATHER"] = dataTable.Rows[j]["WEATHER"].ToString();

                //if (dataTable.Rows[j]["DURATION"].ToString().Trim() == "")
                //{
                //    dr["DURATION"] = 0;
                //}
                //else
                //{
                //    dr["DURATION"] = double.Parse(dataTable.Rows[j]["DURATION"].ToString());
                //}

                dr["LOGINID"] = a1;
                a1++;
                table_new.Rows.Add(dr);
            }
        }




        //for (int i = 0; i < table_new.Rows.Count; i++)
        //{
        //    int abc = table_new.Columns.Count;
        //    for (int k = 0; k < table_new.Columns.Count; k++)
        //    {
        //        if (table_new.Rows[i][k].ToString().IndexOf("日期") != -1)
        //        {
        //            table_new.Columns[k].ReadOnly = false;

        //        }
        //    }
        //}

        //int weeknum = getWeek("2021年05月07日");

        DataRow dr1 = table_new.NewRow();
        table_new.Rows.Add(dr1);

        string Remark = "";

        for (int k = 0; k < table_new.Columns.Count; k++)
        {
            if (table_new.Columns[k].ToString().IndexOf("日期") != -1)
            {
                int LineP = 0;
                table_new.Columns[k].ReadOnly = false;
                for (int i = 1; i < table_new.Rows.Count; i++)
                {
                    string Columnsa = table_new.Columns[k].ToString();
                    if (table_new.Rows[i][Columnsa].ToString() != "")
                    {
                        LineP += 1;
                    }
                }
                //LineP = LineP - 1;
                table_new.Rows[table_new.Rows.Count - 1][k] = LineP;

                string[] WeekI = new string[] { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" };

                if (LineP < 4)
                {
                    int Weekint = getWeek(table_new.Columns[k].ToString().Replace("日期", ""));
                    //周一到周五
                    if (Weekint < 5 && Weekint != 0)
                    {
                        //Remark
                        string Remarkstr = table_new.Columns[k].ToString();
                        Remark += Remarkstr.Replace("日期", "") + "（" + WeekI[Weekint] + "）签到人数：" + LineP + "；";
                    }
                    else
                    {
                        if (LineP < 2)
                        {
                            string Remarkstr = table_new.Columns[k].ToString();
                            Remark += Remarkstr.Replace("日期", "") + "（" + WeekI[Weekint] + "）签到人数：" + LineP + "；";
                        }
                    }
                }
            }
        }
        table_new.Rows[table_new.Rows.Count - 1]["WEATHER"] = Remark;

        //WEATHER


        //for (int i = 0; i < table_new.Rows.Count; i++)
        //{
        //    int sum = 0;

        //    for (int j = 10; j < table_new.Columns.Count - 1; j++)
        //    {

        //        if (int.Parse(table_new.Rows[i][j].ToString()) > 0)
        //        {
        //            sum++;
        //        }
        //    }
        //    //double timesum = 0;
        //    //for (int j = 8; j < table_new.Columns.Count - 1; j++)
        //    //{

        //    //    timesum += double.Parse(table_new.Rows[i][j].ToString());
        //    //}
        //    //table_new.Rows[i]["DURATION"] = timesum.ToString();
        //    table_new.Rows[i]["SIGNINSUM"] = sum;
        //}

        //
        //timesum += double.Parse(table_new.Rows[i][j].ToString());

        //return JsonConvert.SerializeObject(dataTable);
        string jvalue = OracleAccess.DataTableToJson(table_new);
        return jvalue;
    }

    public int getWeek(string DateTimeNow)
    {
        string[] Day = new string[] { "0", "1", "2", "3", "4", "5", "6" };
        string week = Day[Convert.ToInt32(DateTime.Parse(DateTimeNow).DayOfWeek.ToString("d"))].ToString();
        int Weeknum = int.Parse(week);
        return Weeknum;
    }

    /// <summary>
    /// 监管日志 导出报表
    /// </summary>
    /// <param name="sqlText"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Regulatory_log(string type, string starTime, string endTime)
    {

        string sqlText = "select  s.name,to_char(h.UPLOADTIME,'yyyy/mm/dd') RECORDTIME,count(*) LOGCOUNT,weather from (select *  from YH_SUPERVISE_RECORD where uploadtime >= to_date('" + starTime + " 00:00:00', 'yyyy-mm-dd hh24:mi:ss') and uploadtime <= to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss') and TYPE='" + type + "' ) h left join SYS_USERINFO s on h.uploadperson = s.id group by s.name,to_char(h.UPLOADTIME,'yyyy/mm/dd'),weather";
        string Ex = string.Empty;
        OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
        System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

        if (dataSet == null) return string.Empty;
        DataTable dataTable = dataSet.Tables[0];
        if (dataTable == null) return string.Empty;

        //判断时间段中一共有多少天
        System.TimeSpan t3 = DateTime.Parse(endTime) - DateTime.Parse(starTime);

        DataTable table_new = new DataTable();

        //添加列 

        //签到人
        DataColumn day_num1 = new DataColumn();
        day_num1.ColumnName = "SIGNERNAME";
        day_num1.DataType = typeof(string);
        table_new.Columns.Add(day_num1);

        for (int i = 0; i < t3.TotalDays + 1; i++)
        {
            starTime = starTime.Replace("-", "/");
            string testdate = starTime;
            testdate = DateTime.Parse(starTime).ToString("日期yyyy年MM月dd日");
            DataColumn Ctime = new DataColumn();
            Ctime.ColumnName = testdate;
            Ctime.DataType = typeof(string);
            table_new.Columns.Add(Ctime);
            DateTime datetime1 = DateTime.Parse(starTime).AddDays(1);
            starTime = datetime1.ToString("yyyy/MM/dd");

        }

        //LOGINID 序号 
        DataColumn day_num3 = new DataColumn();
        day_num3.ColumnName = "LOGINID";
        day_num3.DataType = typeof(int);
        table_new.Columns.Add(day_num3);

        DataRow dr2 = table_new.NewRow();
        table_new.Rows.Add(dr2);

        int a1 = 2;
        for (int j = 0; j < dataTable.Rows.Count; j++)
        {
            var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["SIGNERNAME"].ToString() == dataTable.Rows[j]["NAME"].ToString());
            if (query1.Count() > 0)
            {
                foreach (var item in query1)
                {
                    string datetime2 = dataTable.Rows[j]["RECORDTIME"].ToString();
                    int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());

                    int countsun = 0;

                    if (table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString() != "")
                    {
                        string aaaa = table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString();
                        countsun = int.Parse(table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString());
                    }

                    if (dataTable.Rows[j]["WEATHER"].ToString() != "")
                    {
                        table_new.Rows[0][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["WEATHER"].ToString();
                    }

                    if (dataTable.Rows[j]["LOGCOUNT"].ToString() == "")
                    {
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = countsun;
                    }
                    else
                    {
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = countsun + int.Parse(dataTable.Rows[j]["LOGCOUNT"].ToString());
                    }

                }
            }
            else
            {
                DataRow dr = table_new.NewRow();
                if (dataTable.Rows[j]["RECORDTIME"].ToString() != "")
                {
                    string datetime2 = dataTable.Rows[j]["RECORDTIME"].ToString();
                    dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["LOGCOUNT"].ToString();

                    if (dataTable.Rows[j]["WEATHER"].ToString() != "")
                    {
                        table_new.Rows[0][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["WEATHER"].ToString();
                    }

                }

                dr["SIGNERNAME"] = dataTable.Rows[j]["NAME"].ToString();

                dr["LOGINID"] = a1;
                a1++;
                table_new.Rows.Add(dr);
            }
        }


        DataRow dr1 = table_new.NewRow();
        table_new.Rows.Add(dr1);

        string Remark = "";

        for (int k = 0; k < table_new.Columns.Count; k++)
        {
            if (table_new.Columns[k].ToString().IndexOf("日期") != -1)
            {
                int LineP = 0;
                table_new.Columns[k].ReadOnly = false;
                for (int i = 1; i < table_new.Rows.Count; i++)
                {
                    string Columnsa = table_new.Columns[k].ToString();
                    if (table_new.Rows[i][Columnsa].ToString() != "")
                    {
                        LineP += 1;
                    }
                }
                table_new.Rows[table_new.Rows.Count - 1][k] = LineP;

                string[] WeekI = new string[] { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" };

                if (LineP < 4)
                {
                    int Weekint = getWeek(table_new.Columns[k].ToString().Replace("日期", ""));
                    //周一到周五
                    if (Weekint < 5 && Weekint != 0)
                    {
                        //Remark
                        string Remarkstr = table_new.Columns[k].ToString();
                        Remark += Remarkstr.Replace("日期", "") + "（" + WeekI[Weekint] + "）填写人数：" + LineP + "；";
                    }
                    else
                    {
                        if (LineP < 2)
                        {
                            string Remarkstr = table_new.Columns[k].ToString();
                            Remark += Remarkstr.Replace("日期", "") + "（" + WeekI[Weekint] + "）填写人数：" + LineP + "；";
                        }
                    }
                }

            }
        }

        table_new.Rows[table_new.Rows.Count - 1]["SIGNERNAME"] = Remark;

        string jvalue = OracleAccess.DataTableToJson(table_new);
        return jvalue;
    }

    /// <summary>
    /// 抽样检查 导出报表
    /// </summary>
    /// <param name="sqlText"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Sampling_log(string starTime, string endTime)
    {

        string sqlText = "select SAMPLINGPEOPLENAME,to_char(SAMPLINGTIME,'yyyy-mm-dd') SAMPLINGTIME,count(*) LOGCOUNT from (select t1.*,decode(t2.STREET,null,t3.TASKID,t2.STREET) as TASKNAME, decode(t2.MAINTAINPERSON,null,t3.TASKORNAME,t2.MAINTAINPERSON) as MAINTAINPERSON from YH_TASK_SAMPLING t1 left join YH_PIPELINE_TASK t2 on t1.TASKID = t2.ID left join YH_PUMP_STATION_TASK t3 on t1.TASKID = t3.TASKID  where SAMPLINGTIME>=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and SAMPLINGTIME<=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss')  ) group by SAMPLINGPEOPLENAME,SAMPLINGTIME";

        string Ex = string.Empty;
        OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
        System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

        if (dataSet == null) return string.Empty;
        DataTable dataTable = dataSet.Tables[0];
        if (dataTable == null) return string.Empty;

        //判断时间段中一共有多少天
        System.TimeSpan t3 = DateTime.Parse(endTime) - DateTime.Parse(starTime);

        DataTable table_new = new DataTable();

        //添加列 

        //签到人
        DataColumn day_num1 = new DataColumn();
        day_num1.ColumnName = "SIGNERNAME";
        day_num1.DataType = typeof(string);
        table_new.Columns.Add(day_num1);

        for (int i = 0; i < t3.TotalDays + 1; i++)
        {
            starTime = starTime.Replace("-", "/");
            string testdate = starTime;
            testdate = DateTime.Parse(starTime).ToString("日期yyyy年MM月dd日");
            DataColumn Ctime = new DataColumn();
            Ctime.ColumnName = testdate;
            Ctime.DataType = typeof(string);
            table_new.Columns.Add(Ctime);
            DateTime datetime1 = DateTime.Parse(starTime).AddDays(1);
            starTime = datetime1.ToString("yyyy/MM/dd");

        }

        //LOGINID 序号 
        DataColumn day_num3 = new DataColumn();
        day_num3.ColumnName = "LOGINID";
        day_num3.DataType = typeof(int);
        table_new.Columns.Add(day_num3);

        DataRow dr2 = table_new.NewRow();
        table_new.Rows.Add(dr2);

        int a1 = 2;
        for (int j = 0; j < dataTable.Rows.Count; j++)
        {
            var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["SIGNERNAME"].ToString() == dataTable.Rows[j]["SAMPLINGPEOPLENAME"].ToString());
            if (query1.Count() > 0)
            {
                foreach (var item in query1)
                {
                    string datetime2 = dataTable.Rows[j]["SAMPLINGTIME"].ToString();
                    int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());

                    int countsun = 0;

                    if (table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString() != "")
                    {
                        string aaaa = table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString();
                        countsun = int.Parse(table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString());
                    }

                    //if (dataTable.Rows[j]["WEATHER"].ToString() != "")
                    //{
                    //    table_new.Rows[0][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["WEATHER"].ToString();
                    //}

                    if (dataTable.Rows[j]["LOGCOUNT"].ToString() == "")
                    {
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = countsun;
                    }
                    else
                    {
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = countsun + int.Parse(dataTable.Rows[j]["LOGCOUNT"].ToString());
                    }

                }
            }
            else
            {
                DataRow dr = table_new.NewRow();
                if (dataTable.Rows[j]["SAMPLINGTIME"].ToString() != "")
                {
                    string datetime2 = dataTable.Rows[j]["SAMPLINGTIME"].ToString();
                    dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["LOGCOUNT"].ToString();

                    //if (dataTable.Rows[j]["WEATHER"].ToString() != "")
                    //{
                    //    table_new.Rows[0][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["WEATHER"].ToString();
                    //}

                }

                dr["SIGNERNAME"] = dataTable.Rows[j]["SAMPLINGPEOPLENAME"].ToString();

                dr["LOGINID"] = a1;
                a1++;
                table_new.Rows.Add(dr);
            }
        }


        DataRow dr1 = table_new.NewRow();
        table_new.Rows.Add(dr1);

        for (int k = 0; k < table_new.Columns.Count; k++)
        {
            if (table_new.Columns[k].ToString().IndexOf("日期") != -1)
            {
                int LineP = 0;
                table_new.Columns[k].ReadOnly = false;
                for (int i = 1; i < table_new.Rows.Count; i++)
                {
                    string Columnsa = table_new.Columns[k].ToString();
                    if (table_new.Rows[i][Columnsa].ToString() != "")
                    {
                        LineP += 1;
                    }
                }
                table_new.Rows[table_new.Rows.Count - 1][k] = LineP;

            }
        }

        string jvalue = OracleAccess.DataTableToJson(table_new);


        return jvalue;
    }

    /// <summary>
    /// 定时上报 导出报表
    /// </summary>
    /// <param name="sqlText"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Regular_report(string starTime, string endTime)
    {
        string sqlText = "select t1.name as USERNAME,t.WEATHER,to_char(t.UPLOADTIME,'yyyy-mm-dd') UPLOADTIME,COUNT(*) LOGCOUNT from (select * from yh_regular_report where UPLOADTIME>=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and UPLOADTIME<=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss') ) t left join SYS_USERINFO t1 on t.uploadperson = t1.id  group by t1.name,t.WEATHER,to_char(t.UPLOADTIME,'yyyy-mm-dd')";
        string Ex = string.Empty;
        OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
        System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

        if (dataSet == null) return string.Empty;
        DataTable dataTable = dataSet.Tables[0];
        if (dataTable == null) return string.Empty;

        //判断时间段中一共有多少天
        System.TimeSpan t3 = DateTime.Parse(endTime) - DateTime.Parse(starTime);

        DataTable table_new = new DataTable();

        //添加列 

        //签到人
        DataColumn day_num1 = new DataColumn();
        day_num1.ColumnName = "USERNAME";
        day_num1.DataType = typeof(string);
        table_new.Columns.Add(day_num1);

        for (int i = 0; i < t3.TotalDays + 1; i++)
        {
            starTime = starTime.Replace("-", "/");
            string testdate = starTime;
            testdate = DateTime.Parse(starTime).ToString("日期yyyy年MM月dd日");
            DataColumn Ctime = new DataColumn();
            Ctime.ColumnName = testdate;
            Ctime.DataType = typeof(string);
            table_new.Columns.Add(Ctime);
            DateTime datetime1 = DateTime.Parse(starTime).AddDays(1);
            starTime = datetime1.ToString("yyyy/MM/dd");

        }

        //LOGINID 序号 
        DataColumn day_num3 = new DataColumn();
        day_num3.ColumnName = "LOGINID";
        day_num3.DataType = typeof(int);
        table_new.Columns.Add(day_num3);

        DataRow dr2 = table_new.NewRow();
        table_new.Rows.Add(dr2);

        int a1 = 2;
        for (int j = 0; j < dataTable.Rows.Count; j++)
        {
            var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["USERNAME"].ToString() == dataTable.Rows[j]["USERNAME"].ToString());
            if (query1.Count() > 0)
            {
                foreach (var item in query1)
                {
                    string datetime2 = dataTable.Rows[j]["UPLOADTIME"].ToString();
                    int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());

                    int countsun = 0;

                    if (table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString() != "")
                    {
                        string aaaa = table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString();
                        countsun = int.Parse(table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")].ToString());
                    }

                    if (dataTable.Rows[j]["WEATHER"].ToString() != "")
                    {
                        table_new.Rows[0][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["WEATHER"].ToString();
                    }

                    if (dataTable.Rows[j]["LOGCOUNT"].ToString() == "")
                    {
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = countsun;
                    }
                    else
                    {
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = countsun + int.Parse(dataTable.Rows[j]["LOGCOUNT"].ToString());
                    }

                }
            }
            else
            {
                DataRow dr = table_new.NewRow();
                if (dataTable.Rows[j]["UPLOADTIME"].ToString() != "")
                {
                    string datetime2 = dataTable.Rows[j]["UPLOADTIME"].ToString();
                    dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["LOGCOUNT"].ToString();

                    if (dataTable.Rows[j]["WEATHER"].ToString() != "")
                    {
                        table_new.Rows[0][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["WEATHER"].ToString();
                    }

                }

                dr["USERNAME"] = dataTable.Rows[j]["USERNAME"].ToString();

                dr["LOGINID"] = a1;
                a1++;
                table_new.Rows.Add(dr);
            }
        }


        DataRow dr1 = table_new.NewRow();
        table_new.Rows.Add(dr1);

        string Remark = "";

        for (int k = 0; k < table_new.Columns.Count; k++)
        {
            if (table_new.Columns[k].ToString().IndexOf("日期") != -1)
            {
                int LineP = 0;
                table_new.Columns[k].ReadOnly = false;
                for (int i = 1; i < table_new.Rows.Count; i++)
                {
                    string Columnsa = table_new.Columns[k].ToString();
                    if (table_new.Rows[i][Columnsa].ToString() != "")
                    {
                        LineP += 1;
                    }
                }
                table_new.Rows[table_new.Rows.Count - 1][k] = LineP;

                string[] WeekI = new string[] { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" };

                if (LineP < 4)
                {
                    int Weekint = getWeek(table_new.Columns[k].ToString().Replace("日期", ""));
                    //周一到周五
                    if (Weekint < 5 && Weekint != 0)
                    {
                        //Remark
                        string Remarkstr = table_new.Columns[k].ToString();
                        Remark += Remarkstr.Replace("日期", "") + "（" + WeekI[Weekint] + "）填写人数：" + LineP + "；";
                    }
                    else
                    {
                        if (LineP < 2)
                        {
                            string Remarkstr = table_new.Columns[k].ToString();
                            Remark += Remarkstr.Replace("日期", "") + "（" + WeekI[Weekint] + "）填写人数：" + LineP + "；";
                        }
                    }
                }

            }
        }

        table_new.Rows[table_new.Rows.Count - 1]["USERNAME"] = Remark;

        string jvalue = OracleAccess.DataTableToJson(table_new);
        return jvalue;
    }





    /// <summary>
    /// 巡查信息
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <param name="sqlpqsx"></param>
    /// <returns></returns>
    [OperationContract]
    public string Inspections_info(string keyword, string starTime, string endTime, string sqlpqsx, string Companys)
    {
        try
        {
            string sqlText = " ";
            string sqlArea = "";
            string pqsql = " ";

            if (sqlpqsx != "")
            {
                pqsql = " and t3.subarea = '" + sqlpqsx + "'";
            }
            //sqlText = "select t4.ID,t4.name,t1.subarea,t1.Status,to_char(finishtime,'yyyy-mm-dd') FINISHTIME,COUNT(*) INSPNUM from (select * from YH_PUMP_STATION_TASK where finishtime>=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and finishtime<=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss') ) t1 inner join YH_PUMP_STATION_PLAN t2 on t1.planid=t2.planid inner join YH_PATROL_ROUTE t3 on t2.planroutine=t3.id inner join YH_STREETS t4 on t3.involvedroad=t4.id group by  t4.ID,t4.name,t1.subarea,t1.Status,to_char(finishtime,'yyyy-mm-dd') having t1.Status='1' and t4.name like '%" + keyword + "%'";
            sqlText = "select t3.ID,t3.NAME,t2.Subarea,t1.STATUS,to_char(t1.FINISHTIME,'yyyy-mm-dd') FINISHTIME,count(*) INSPNUM,t3.CONSTYPE from YH_TASK_STREET t1 inner join (select * from YH_PUMP_STATION_TASK where finishtime>=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and finishtime<=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss')) t2 on t1.taskid=t2.TASKID inner join YH_STREETS t3 on t1.STREETID=t3.id group by t3.ID,t3.NAME,t2.Subarea,t1.STATUS,to_char(t1.FINISHTIME,'yyyy-mm-dd'),t3.CONSTYPE  having t1.Status='2' and t3.name like '%" + keyword + "%' ";

            sqlText = " select t3.id,t3.subarea,t3.name,t1.status,to_char(t1.FINISHTIME,'yyyy-mm-dd') FINISHTIME,count(*) INSPNUM  from  ((select * from YH_TASK_STREET where finishtime>=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and finishtime<=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss')) t1 " +
  "inner join " +
  " (select * from YH_PUMP_STATION_TASK ) t2 on t1.taskid=t2.TASKID " +
   " inner join " +
   " YH_STREETS t3 on t1.STREETID=t3.id " +
  " )" +
  " group by t3.id,t3.subarea,t3.name,t1.status,to_char(t1.FINISHTIME,'yyyy-mm-dd'),t3.CONSTYPE " +
  "having t3.name like '%" + keyword + "%' order by t3.subarea,t3.name";

            sqlText = " select t3.id,t3.subarea,t3.name,t2.COMPANYID,t1.status,to_char(t1.FINISHTIME,'yyyy-mm-dd') FINISHTIME,count(*) INSPNUM,SUM(duration) duration from  ((select taskid,STREETID,status, FINISHTIME,STARTTIME,ceil((FINISHTIME-STARTTIME)*24*60*60) duration from YH_TASK_STREET where finishtime>=to_date('" + starTime + " 00:00:00','yyyy-mm-dd hh24:mi:ss') and finishtime<=to_date('" + endTime + " 23:59:59','yyyy-mm-dd hh24:mi:ss')) t1  inner join  (select * from YH_PUMP_STATION_TASK ) t2 on t1.taskid=t2.TASKID  inner join  YH_STREETS t3 on t1.STREETID=t3.id  ) group by t3.id,t3.subarea,t2.COMPANYID,t3.name,t1.status,to_char(t1.FINISHTIME,'yyyy-mm-dd'),t3.CONSTYPE having t3.name like '%" + keyword + "%'  " + Companys + " order by t3.subarea,t3.name";

            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;

            //判断时间段中一共有多少天
            System.TimeSpan t3 = DateTime.Parse(endTime) - DateTime.Parse(starTime);
            if (sqlpqsx != "")
            {
                sqlArea = " and SUBAREA='" + sqlpqsx + "' ";
            }
            string sqlText1 = "select ROWNUM,s.ID,NAME,SUBAREA,CONSTYPE,a.Areaid from YH_STREETS s left join SYS_AREAS a on s.Subarea=a.areaname where NAME like '%" + keyword + "%'" + sqlpqsx;
            System.Data.DataSet dataSet1 = oracleAccess.QueryTableInfo(sqlText1, out Ex);
            if (dataSet1 == null) return string.Empty;
            DataTable table_new = dataSet1.Tables[0];
            if (dataTable == null) return string.Empty;
            //DataTable table_new = new DataTable();

            //添加列 

            //道路名称
            //DataColumn day_num = new DataColumn();
            //day_num.ColumnName = "NAME";
            //day_num.DataType = typeof(string);
            //table_new.Columns.Add(day_num);
            //所属片区
            //DataColumn day_num_ID = new DataColumn();
            //day_num_ID.ColumnName = "SUBAREA";
            //day_num_ID.DataType = typeof(string);
            //table_new.Columns.Add(day_num_ID);

            ////巡查时间
            //DataColumn day_num1 = new DataColumn();
            //day_num1.ColumnName = "PATROLTIME";
            //day_num1.DataType = typeof(date);
            //table_new.Columns.Add(day_num1);

            //巡查总时长
            DataColumn day_num5 = new DataColumn();
            day_num5.ColumnName = "DURATION";
            day_num5.DataType = typeof(double);
            table_new.Columns.Add(day_num5);

            //巡查总数
            DataColumn day_num4 = new DataColumn();
            day_num4.ColumnName = "INSPNUM";
            day_num4.DataType = typeof(double);
            table_new.Columns.Add(day_num4);

            for (int i = 0; i < t3.TotalDays + 1; i++)
            {
                endTime = endTime.Replace("-", "/");
                string testdate = endTime;
                testdate = DateTime.Parse(endTime).ToString("日期yyyy年MM月dd日");
                DataColumn Ctime = new DataColumn();
                Ctime.ColumnName = testdate;
                Ctime.DataType = typeof(string);
                table_new.Columns.Add(Ctime);
                DateTime datetime1 = DateTime.Parse(endTime).AddDays(-1);
                endTime = datetime1.ToString("yyyy/MM/dd");

            }

            //LOGINID 序号 
            //DataColumn day_num3 = new DataColumn();
            //day_num3.ColumnName = "LOGINID";
            //day_num3.DataType = typeof(int);
            //table_new.Columns.Add(day_num3);

            for (int j = 0; j < dataTable.Rows.Count; j++)
            {
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["ID"].ToString() == dataTable.Rows[j]["ID"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {
                        string datetime2 = dataTable.Rows[j]["FINISHTIME"].ToString();
                        //string aa = item.ItemArray[1].ToString();
                        //string bb = dataTable.Rows[j]["SIGNERNAME"].ToString();
                        int cc = int.Parse(item.ItemArray[0].ToString());
                        //table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["INSPNUM"].ToString();
                        table_new.Rows[cc - 1][DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["DURATION"].ToString();
                        //table_new.Rows[cc - 1]["SUBAREA"] = dataTable.Rows[j]["SUBAREA"].ToString();
                        //table_new.Rows[cc - 1]["FINISHTIME"] = dataTable.Rows[j]["FINISHTIME"].ToString();
                        //table_new.Rows[cc - 1]["INSPNUM"] = dataTable.Rows[j]["INSPNUM"].ToString();
                    }
                }
                //else
                //{
                //    DataRow dr = table_new.NewRow();
                //    if (dataTable.Rows[j]["FINISHTIME"].ToString() != "")
                //    {
                //        string datetime2 = dataTable.Rows[j]["FINISHTIME"].ToString();
                //        dr[DateTime.Parse(datetime2).ToString("日期yyyy年MM月dd日")] = dataTable.Rows[j]["INSPNUM"].ToString();
                //    }
                //    dr["NAME"] = dataTable.Rows[j]["NAME"].ToString();
                //    dr["SUBAREA"] = dataTable.Rows[j]["SUBAREA"].ToString();
                //    //dr["FINISHTIME"] = dataTable.Rows[j]["FINISHTIME"].ToString();
                //    //dr["INSPNUM"] = dataTable.Rows[j]["INSPNUM"].ToString();
                //    //dr["LOGINID"] = a1;
                //    a1++;
                //    table_new.Rows.Add(dr);
                //}
            }


            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int abc = table_new.Columns.Count;
                for (int k = 0; k < table_new.Columns.Count; k++)
                {
                    if (table_new.Rows[i][k].ToString() == "")
                    {
                        table_new.Rows[i][k] = "0";
                    }
                }
            }

            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                double sum = 0;
                double Count = 0;
                for (int j = 7; j < table_new.Columns.Count; j++)
                {
                    sum += double.Parse(table_new.Rows[i][j].ToString());
                    if (double.Parse(table_new.Rows[i][j].ToString()) > 0)
                    {
                        Count++;
                    }
                }
                table_new.Rows[i]["DURATION"] = sum;
                table_new.Rows[i]["INSPNUM"] = Count;
            }
            //return JsonConvert.SerializeObject(dataTable);
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }

    //养护计划
    [OperationContract]
    public string Maintenance_plan(string keyword, string starTime, string endTime, string type, string AREA)
    {
        try
        {
            string timesql = "";
            if (starTime.Trim() != "" && endTime.Trim() != "")
            {
                timesql = " where CREATETIME >= to_date('" + starTime + "','yyyy-mm-dd hh24:mi:ss') and CREATETIME <= to_date('" + endTime + "','yyyy-mm-dd hh24:mi:ss') ";
            }
            string status_sql = "";
            if (int.Parse(type) >= 0)
            {
                status_sql += " and t1.STATUS='" + type + "' ";
            }

            if (AREA.Trim() != "全部")
            {
                status_sql += " and t1.SUBAREA='" + AREA.Trim() + "' ";
            }


            string sqlText = "select t1.ID,t1.RECORD,t1.PROJECTLIMITTIME,t1.CREATETIME,t1.year,t1.month,t1.season,t1.roadname,t1.STATUS,t1.SUBAREA,t1.SUBTYPE,t2.Welltype,SUM(t2.Wellamount) WELLSUM from (select * from YH_PLAN  " + timesql + ") t1 left join YH_PLAN_WELL_INFO t2 on t1.id=t2.taskid group by  t1.year,t1.month,t1.season,t1.roadname,t2.Welltype, t1.ID, t1.RECORD,t1.CREATETIME,t1.PROJECTLIMITTIME,t1.STATUS,t1.SUBAREA,t1.SUBTYPE  having t1.roadname like  '%" + keyword.Trim() + "%' " + status_sql + " order by t1.CREATETIME desc ";
            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;


            string sqlText1 = "select  t1.ID,t1.RECORD,t1.CREATETIME,t1.year,t1.month,t1.season,t1.roadname,t2.PIPETYPE,t2.PIPEDIAMETER,t1.STATUS,t1.SUBAREA,t1.SUBTYPE,SUM(t2.AMOUNT) PIRLSUM from (select * from YH_PLAN  " + timesql + " ) t1 left join YH_PLAN_PIPELINE_INFO t2 on t1.id=t2.taskid group by   t1.year,t1.month,t1.season,t1.roadname,t2.PIPETYPE,t2.PIPEDIAMETER,t1.ID,t1.RECORD,t1.CREATETIME,t1.PROJECTLIMITTIME,t1.STATUS,t1.SUBAREA,t1.SUBTYPE having t1.CREATETIME like '%" + keyword.Trim() + "%' " + status_sql + "  order by t1.roadname  desc";
            System.Data.DataSet dataSet1 = oracleAccess.QueryTableInfo(sqlText1, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable1 = dataSet1.Tables[0];
            if (dataTable == null) return string.Empty;

            DataTable table_new = new DataTable();

            //添加列 

            //养护ID
            DataColumn day_num3 = new DataColumn();
            day_num3.ColumnName = "ID";
            day_num3.DataType = typeof(int);
            table_new.Columns.Add(day_num3);

            //养护年份
            DataColumn day_num = new DataColumn();
            day_num.ColumnName = "YEAR";
            day_num.DataType = typeof(int);
            table_new.Columns.Add(day_num);

            //养护状态
            DataColumn day_num_STATUS = new DataColumn();
            day_num_STATUS.ColumnName = "STATUS";
            day_num_STATUS.DataType = typeof(string);
            table_new.Columns.Add(day_num_STATUS);

            //养护月份
            DataColumn day_num_ID = new DataColumn();
            day_num_ID.ColumnName = "MONTH";
            day_num_ID.DataType = typeof(int);
            table_new.Columns.Add(day_num_ID);
            //预计工期
            DataColumn day_num_PROJECTLIMITTIME = new DataColumn();
            day_num_PROJECTLIMITTIME.ColumnName = "PROJECTLIMITTIME";
            day_num_PROJECTLIMITTIME.DataType = typeof(int);
            table_new.Columns.Add(day_num_PROJECTLIMITTIME);
            //养护季度
            DataColumn day_num1 = new DataColumn();
            day_num1.ColumnName = "SEASON";
            day_num1.DataType = typeof(string);
            table_new.Columns.Add(day_num1);

            //街道及小区名称
            DataColumn day_num1_ID = new DataColumn();
            day_num1_ID.ColumnName = "ROADNAME";
            day_num1_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num1_ID);

            //片区
            DataColumn day_pq = new DataColumn();
            day_pq.ColumnName = "SUBAREA";
            day_pq.DataType = typeof(string);
            table_new.Columns.Add(day_pq);
            //计划子类
            DataColumn jhzl = new DataColumn();
            jhzl.ColumnName = "SUBTYPE";
            jhzl.DataType = typeof(string);
            table_new.Columns.Add(jhzl);

            //创建时间
            DataColumn day_num1_TIME = new DataColumn();
            day_num1_TIME.ColumnName = "CREATETIME";
            day_num1_TIME.DataType = typeof(DateTime);
            table_new.Columns.Add(day_num1_TIME);
            //备注
            DataColumn day_num1_RECORD = new DataColumn();
            day_num1_RECORD.ColumnName = "RECORD";
            day_num1_RECORD.DataType = typeof(string);
            table_new.Columns.Add(day_num1_RECORD);


            //DN300以下雨水管道
            DataColumn day_num4 = new DataColumn();
            day_num4.ColumnName = "DN300以下雨水管道";
            day_num4.DataType = typeof(int);
            table_new.Columns.Add(day_num4);

            //DN300以下雨水管道
            DataColumn day_num5 = new DataColumn();
            day_num5.ColumnName = "DN300以下污水管道";
            day_num5.DataType = typeof(int);
            table_new.Columns.Add(day_num5);

            //DN300-600雨水管道
            DataColumn day_num6 = new DataColumn();
            day_num6.ColumnName = "DN300至600雨水管道";
            day_num6.DataType = typeof(int);
            table_new.Columns.Add(day_num6);

            //DN300-600污水管道
            DataColumn day_num7 = new DataColumn();
            day_num7.ColumnName = "DN300至600污水管道";
            day_num7.DataType = typeof(int);
            table_new.Columns.Add(day_num7);

            //DN600以上雨水管道
            DataColumn day_num8 = new DataColumn();
            day_num8.ColumnName = "DN600以上雨水管道";
            day_num8.DataType = typeof(int);
            table_new.Columns.Add(day_num8);

            //DN600以上污水管道
            DataColumn day_num9 = new DataColumn();
            day_num9.ColumnName = "DN600以上污水管道";
            day_num9.DataType = typeof(int);
            table_new.Columns.Add(day_num9);

            //雨水检查井
            DataColumn day_num10 = new DataColumn();
            day_num10.ColumnName = "雨水检查井";
            day_num10.DataType = typeof(int);
            table_new.Columns.Add(day_num10);

            //污水检查井
            DataColumn day_num11 = new DataColumn();
            day_num11.ColumnName = "污水检查井";
            day_num11.DataType = typeof(int);
            table_new.Columns.Add(day_num11);

            //雨水收水井
            DataColumn day_num12 = new DataColumn();
            day_num12.ColumnName = "雨水收水井";
            day_num12.DataType = typeof(int);
            table_new.Columns.Add(day_num12);

            //化粪池
            DataColumn day_num13 = new DataColumn();
            day_num13.ColumnName = "化粪池";
            day_num13.DataType = typeof(int);
            table_new.Columns.Add(day_num13);

            //边井
            DataColumn day_num14 = new DataColumn();
            day_num14.ColumnName = "边井";
            day_num14.DataType = typeof(int);
            table_new.Columns.Add(day_num14);
            //边井
            DataColumn day_num15 = new DataColumn();
            day_num15.ColumnName = "隔油池";
            day_num15.DataType = typeof(int);
            table_new.Columns.Add(day_num15);

            //检查井总数
            DataColumn day_num_WSUM = new DataColumn();
            day_num_WSUM.ColumnName = "WSUM";
            day_num_WSUM.DataType = typeof(int);
            table_new.Columns.Add(day_num_WSUM);
            //管道总长度
            DataColumn day_num_PSUM = new DataColumn();
            day_num_PSUM.ColumnName = "PSUM";
            day_num_PSUM.DataType = typeof(int);
            table_new.Columns.Add(day_num_PSUM);

            //LOGINID 序号 
            DataColumn day_num16 = new DataColumn();
            day_num16.ColumnName = "LOGINID";
            day_num16.DataType = typeof(int);
            table_new.Columns.Add(day_num16);

            int a1 = 1;
            for (int j = 0; j < dataTable.Rows.Count; j++)
            {
                if (dataTable.Rows[j]["WELLTYPE"].ToString() == "化粪池")
                {
                    string aaa = dataTable.Rows[j]["WELLTYPE"].ToString();
                }
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["ID"].ToString() == dataTable.Rows[j]["ID"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {

                        int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());

                        if (dataTable.Rows[j]["WELLTYPE"].ToString() != "")
                        {
                            string aavv = dataTable.Rows[j]["WELLTYPE"].ToString();
                            table_new.Rows[cc - 1][dataTable.Rows[j]["WELLTYPE"].ToString()] = int.Parse(dataTable.Rows[j]["WELLSUM"].ToString());
                        }

                    }
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    dr["ID"] = dataTable.Rows[j]["ID"].ToString();
                    dr["YEAR"] = dataTable.Rows[j]["YEAR"].ToString();
                    dr["MONTH"] = dataTable.Rows[j]["MONTH"].ToString();
                    dr["SEASON"] = dataTable.Rows[j]["SEASON"].ToString();
                    dr["ROADNAME"] = dataTable.Rows[j]["ROADNAME"].ToString();
                    dr["SUBAREA"] = dataTable.Rows[j]["SUBAREA"].ToString();
                    dr["SUBTYPE"] = dataTable.Rows[j]["SUBTYPE"].ToString();
                    dr["CREATETIME"] = dataTable.Rows[j]["CREATETIME"].ToString();
                    dr["RECORD"] = dataTable.Rows[j]["RECORD"].ToString();
                    dr["PROJECTLIMITTIME"] = dataTable.Rows[j]["PROJECTLIMITTIME"].ToString();
                    if (dataTable.Rows[j]["STATUS"].ToString() == "0")
                    {
                        dr["STATUS"] = "待审核";
                    }
                    if (dataTable.Rows[j]["STATUS"].ToString() == "1")
                    {
                        dr["STATUS"] = "已通过";
                    }
                    if (dataTable.Rows[j]["STATUS"].ToString() == "2")
                    {
                        dr["STATUS"] = "未通过";
                    }
                    if (dataTable.Rows[j]["STATUS"].ToString() == "3")
                    {
                        dr["STATUS"] = "已发单";
                    }
                    //PROJECTLIMITTIME
                    dr["LOGINID"] = a1;
                    a1++;
                    table_new.Rows.Add(dr);
                    for (int i = 0; i < table_new.Rows.Count; i++)
                    {

                        if (dataTable.Rows[j]["ID"].ToString() == table_new.Rows[i]["ID"].ToString())
                        {
                            string aavv = dataTable.Rows[j]["WELLTYPE"].ToString();
                            if (dataTable.Rows[j]["WELLTYPE"].ToString() != "")
                            {
                                table_new.Rows[i][dataTable.Rows[j]["WELLTYPE"].ToString()] = int.Parse(dataTable.Rows[j]["WELLSUM"].ToString());
                            }

                        }
                    }
                }

                //var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["ID"].ToString() == dataTable.Rows[j]["ID"].ToString());
                //if (query1.Count() > 0) { 

                //}
                //else
                //{

                //}

                //DataRow dr = table_new.NewRow();
                //dr["ID"] = dataTable.Rows[j]["ID"].ToString();
                //dr["YEAR"] = dataTable.Rows[j]["YEAR"].ToString();
                //dr["MONTH"] = dataTable.Rows[j]["MONTH"].ToString();
                //dr["SEASON"] = dataTable.Rows[j]["SEASON"].ToString();
                //dr["ROADNAME"] = dataTable.Rows[j]["ROADNAME"].ToString();
                //table_new.Rows.Add(dr);

                //for (int i = 0; i < table_new.Rows.Count; i++)
                //{
                //    if (dataTable.Rows[j]["ID"] == table_new.Rows[i]["ID"])
                //    {
                //        table_new.Rows[i][dataTable.Rows[j]["WELLTYPE"].ToString()] = int.Parse(dataTable.Rows[j]["WELLSUM"].ToString());
                //    }
                //}

            }

            for (int j = 0; j < dataTable1.Rows.Count; j++)
            {
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["ID"].ToString() == dataTable1.Rows[j]["ID"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {

                        int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());
                        string index_name = dataTable1.Rows[j]["PIPEDIAMETER"].ToString() + dataTable1.Rows[j]["PIPETYPE"].ToString();
                        index_name = index_name.Trim();
                        index_name = index_name.Replace("-", "至");
                        if (index_name != "")
                        {
                            table_new.Rows[cc - 1][index_name] = int.Parse(dataTable1.Rows[j]["PIRLSUM"].ToString());
                        }

                    }
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    dr["ID"] = dataTable1.Rows[j]["ID"].ToString();
                    dr["YEAR"] = dataTable1.Rows[j]["YEAR"].ToString();
                    dr["MONTH"] = dataTable1.Rows[j]["MONTH"].ToString();
                    dr["SEASON"] = dataTable1.Rows[j]["SEASON"].ToString();
                    dr["ROADNAME"] = dataTable1.Rows[j]["ROADNAME"].ToString();
                    dr["SUBAREA"] = dataTable.Rows[j]["SUBAREA"].ToString();
                    dr["SUBTYPE"] = dataTable.Rows[j]["SUBTYPE"].ToString();
                    dr["CREATETIME"] = dataTable.Rows[j]["CREATETIME"].ToString();
                    dr["RECORD"] = dataTable.Rows[j]["RECORD"].ToString();
                    dr["PROJECTLIMITTIME"] = dataTable.Rows[j]["PROJECTLIMITTIME"].ToString();
                    if (dataTable.Rows[j]["STATUS"].ToString() == "1")
                    {
                        dr["STATUS"] = "已发单";
                    }
                    else
                    {
                        dr["STATUS"] = "未发单";
                    }

                    dr["LOGINID"] = a1;
                    a1++;
                    table_new.Rows.Add(dr);
                    if (table_new.Rows.Count < dataTable1.Rows.Count)
                    {
                        for (int i = 0; i < table_new.Rows.Count; i++)
                        {
                            if (dataTable1.Rows[j]["ID"] == table_new.Rows[i]["ID"])
                            {
                                string index_name = dataTable1.Rows[j]["PIPEDIAMETER"].ToString() + dataTable1.Rows[j]["PIPETYPE"].ToString();
                                index_name = index_name.Trim();
                                table_new.Rows[i][index_name] = int.Parse(dataTable1.Rows[j]["PIRLSUM"].ToString());
                            }
                        }
                    }

                }
                //DataRow dr = table_new.NewRow();
                //dr["ID"] = dataTable1.Rows[j]["ID"].ToString();
                //dr["YEAR"] = dataTable1.Rows[j]["YEAR"].ToString();
                //dr["MONTH"] = dataTable1.Rows[j]["MONTH"].ToString();
                //dr["SEASON"] = dataTable1.Rows[j]["SEASON"].ToString();
                //dr["ROADNAME"] = dataTable1.Rows[j]["ROADNAME"].ToString();
                //table_new.Rows.Add(dr);

            }

            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int abc = table_new.Columns.Count;
                for (int k = 0; k < table_new.Columns.Count; k++)
                {
                    if (table_new.Rows[i][k].ToString() == "")
                    {
                        table_new.Rows[i][k] = "0";
                    }
                }
            }
            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int WSUM = 0;
                int PSUM = 0;
                for (int j = 11; j < table_new.Columns.Count - 1; j++)
                {
                    if (j < 17)
                    {
                        PSUM += int.Parse(table_new.Rows[i][j].ToString());
                    }
                    else
                    {
                        WSUM += int.Parse(table_new.Rows[i][j].ToString());
                    }
                }
                table_new.Rows[i]["WSUM"] = WSUM;
                table_new.Rows[i]["PSUM"] = PSUM;
            }

            //return JsonConvert.SerializeObject(dataTable);
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }

    //养护任务
    [OperationContract]
    public string Maintenance_task(string keyword, string starTime, string endTime, string type, string AREA)
    {
        try
        {
            string timesql = "";
            if (starTime.Trim() != "" && endTime.Trim() != "")
            {
                timesql = " where CREATETIME >= to_date('" + starTime + "','yyyy-mm-dd hh24:mi:ss') and CREATETIME <= to_date('" + endTime + "','yyyy-mm-dd hh24:mi:ss') ";
            }

            string status_sql = "";
            if (type.Trim() != "全部")
            {
                status_sql += " and t1.SUBTYPE='" + type.Trim() + "' ";
            }

            if (AREA.Trim() != "全部")
            {
                status_sql += " and t1.SUBAREA='" + AREA.Trim() + "' ";
            }

            string sqlText = "select t1.ID,t1.RECORD,t1.PROJECTLIMITTIME,t1.CREATETIME,t1.year,t1.month,t1.season,t1.roadname,t2.Welltype,t1.SUBAREA,t1.SUBTYPE,COUNT(*) WELLSUM from (select * from YH_PLAN) t1 inner join (select * from YH_PIPELINE_TASK   " + timesql + " ) t3 on t1.ID=t3.PLANID  left join (select * from YH_WELL_INFO where STATUS='1') t2 on t1.id=t2.taskid group by  t1.year,t1.month,t1.season,t1.roadname,t2.Welltype, t1.ID, t1.RECORD,t1.CREATETIME,t1.PROJECTLIMITTIME,t1.SUBAREA,t1.SUBTYPE  having t1.roadname like  '%" + keyword.Trim() + "%' " + status_sql + " order by t1.CREATETIME desc";
            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;


            string sqlText1 = "select  t1.ID,t1.RECORD,t1.CREATETIME,t1.year,t1.month,t1.season,t1.roadname,t2.PIPETYPE,t2.PIPEDIAMETER,t1.SUBAREA,t1.SUBTYPE,SUM(t2.AMOUNT) PIRLSUM from (select * from YH_PLAN ) t1 inner join (select * from YH_PIPELINE_TASK   " + timesql + " ) t3 on t1.ID=t3.PLANID left join ( select * from YH_PIPELINE_INFO where STATUS='1' ) t2 on t1.id=t2.taskid group by   t1.year,t1.month,t1.season,t1.roadname,t2.PIPETYPE,t2.PIPEDIAMETER,t1.ID,t1.RECORD,t1.CREATETIME,t1.PROJECTLIMITTIME,t1.SUBAREA,t1.SUBTYPE having t1.roadname like '%" + keyword.Trim() + "%' " + status_sql + "  order by t1.CREATETIME  desc";
            System.Data.DataSet dataSet1 = oracleAccess.QueryTableInfo(sqlText1, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable1 = dataSet1.Tables[0];
            if (dataTable == null) return string.Empty;

            DataTable table_new = new DataTable();

            //添加列 

            //养护ID
            DataColumn day_num3 = new DataColumn();
            day_num3.ColumnName = "ID";
            day_num3.DataType = typeof(int);
            table_new.Columns.Add(day_num3);

            //养护年份
            DataColumn day_num = new DataColumn();
            day_num.ColumnName = "YEAR";
            day_num.DataType = typeof(int);
            table_new.Columns.Add(day_num);
            //养护月份
            DataColumn day_num_ID = new DataColumn();
            day_num_ID.ColumnName = "MONTH";
            day_num_ID.DataType = typeof(int);
            table_new.Columns.Add(day_num_ID);
            //预计工期
            DataColumn day_num_PROJECTLIMITTIME = new DataColumn();
            day_num_PROJECTLIMITTIME.ColumnName = "PROJECTLIMITTIME";
            day_num_PROJECTLIMITTIME.DataType = typeof(int);
            table_new.Columns.Add(day_num_PROJECTLIMITTIME);
            //养护季度
            DataColumn day_num1 = new DataColumn();
            day_num1.ColumnName = "SEASON";
            day_num1.DataType = typeof(string);
            table_new.Columns.Add(day_num1);

            //街道及小区名称
            DataColumn day_num1_ID = new DataColumn();
            day_num1_ID.ColumnName = "ROADNAME";
            day_num1_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num1_ID);

            //片区
            DataColumn day_pq = new DataColumn();
            day_pq.ColumnName = "SUBAREA";
            day_pq.DataType = typeof(string);
            table_new.Columns.Add(day_pq);
            //任务子类
            DataColumn jhzl = new DataColumn();
            jhzl.ColumnName = "SUBTYPE";
            jhzl.DataType = typeof(string);
            table_new.Columns.Add(jhzl);

            //创建时间
            DataColumn day_num1_TIME = new DataColumn();
            day_num1_TIME.ColumnName = "CREATETIME";
            day_num1_TIME.DataType = typeof(DateTime);
            table_new.Columns.Add(day_num1_TIME);
            //备注
            DataColumn day_num1_RECORD = new DataColumn();
            day_num1_RECORD.ColumnName = "RECORD";
            day_num1_RECORD.DataType = typeof(string);
            table_new.Columns.Add(day_num1_RECORD);


            //DN300以下雨水管道
            DataColumn day_num4 = new DataColumn();
            day_num4.ColumnName = "DN300以下雨水管道";
            day_num4.DataType = typeof(int);
            table_new.Columns.Add(day_num4);

            //DN300以下雨水管道
            DataColumn day_num5 = new DataColumn();
            day_num5.ColumnName = "DN300以下污水管道";
            day_num5.DataType = typeof(int);
            table_new.Columns.Add(day_num5);

            //DN300-600雨水管道
            DataColumn day_num6 = new DataColumn();
            day_num6.ColumnName = "DN300至600雨水管道";
            day_num6.DataType = typeof(int);
            table_new.Columns.Add(day_num6);

            //DN300-600污水管道
            DataColumn day_num7 = new DataColumn();
            day_num7.ColumnName = "DN300至600污水管道";
            day_num7.DataType = typeof(int);
            table_new.Columns.Add(day_num7);

            //DN600以上雨水管道
            DataColumn day_num8 = new DataColumn();
            day_num8.ColumnName = "DN600以上雨水管道";
            day_num8.DataType = typeof(int);
            table_new.Columns.Add(day_num8);

            //DN600以上污水管道
            DataColumn day_num9 = new DataColumn();
            day_num9.ColumnName = "DN600以上污水管道";
            day_num9.DataType = typeof(int);
            table_new.Columns.Add(day_num9);

            //雨水检查井
            DataColumn day_num10 = new DataColumn();
            day_num10.ColumnName = "雨水检查井";
            day_num10.DataType = typeof(int);
            table_new.Columns.Add(day_num10);

            ////污水检查井
            DataColumn day_num11 = new DataColumn();
            day_num11.ColumnName = "污水检查井";
            day_num11.DataType = typeof(int);
            table_new.Columns.Add(day_num11);

            //雨水收水井
            DataColumn day_num12 = new DataColumn();
            day_num12.ColumnName = "雨水收水井";
            day_num12.DataType = typeof(int);
            table_new.Columns.Add(day_num12);

            //化粪池
            DataColumn day_num13 = new DataColumn();
            day_num13.ColumnName = "化粪池";
            day_num13.DataType = typeof(int);
            table_new.Columns.Add(day_num13);

            //边井
            DataColumn day_num14 = new DataColumn();
            day_num14.ColumnName = "边井";
            day_num14.DataType = typeof(int);
            table_new.Columns.Add(day_num14);
            //边井
            DataColumn day_num15 = new DataColumn();
            day_num15.ColumnName = "隔油池";
            day_num15.DataType = typeof(int);
            table_new.Columns.Add(day_num15);

            //检查井总数
            DataColumn day_num_WSUM = new DataColumn();
            day_num_WSUM.ColumnName = "WSUM";
            day_num_WSUM.DataType = typeof(int);
            table_new.Columns.Add(day_num_WSUM);
            //管道总长度
            DataColumn day_num_PSUM = new DataColumn();
            day_num_PSUM.ColumnName = "PSUM";
            day_num_PSUM.DataType = typeof(int);
            table_new.Columns.Add(day_num_PSUM);

            //LOGINID 序号 
            DataColumn day_num16 = new DataColumn();
            day_num16.ColumnName = "LOGINID";
            day_num16.DataType = typeof(int);
            table_new.Columns.Add(day_num16);

            int a1 = 1;
            for (int j = 0; j < dataTable.Rows.Count; j++)
            {
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["ID"].ToString() == dataTable.Rows[j]["ID"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {

                        int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());

                        if (dataTable.Rows[j]["WELLTYPE"].ToString() != "")
                        {
                            string aavv = dataTable.Rows[j]["WELLTYPE"].ToString();
                            table_new.Rows[cc - 1][dataTable.Rows[j]["WELLTYPE"].ToString()] = int.Parse(dataTable.Rows[j]["WELLSUM"].ToString());
                        }

                    }
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    dr["ID"] = dataTable.Rows[j]["ID"].ToString();
                    dr["YEAR"] = dataTable.Rows[j]["YEAR"].ToString();
                    dr["MONTH"] = dataTable.Rows[j]["MONTH"].ToString();
                    dr["SEASON"] = dataTable.Rows[j]["SEASON"].ToString();
                    dr["ROADNAME"] = dataTable.Rows[j]["ROADNAME"].ToString();
                    dr["SUBAREA"] = dataTable.Rows[j]["SUBAREA"].ToString();
                    dr["SUBTYPE"] = dataTable.Rows[j]["SUBTYPE"].ToString();
                    dr["CREATETIME"] = dataTable.Rows[j]["CREATETIME"].ToString();
                    dr["RECORD"] = dataTable.Rows[j]["RECORD"].ToString();
                    dr["PROJECTLIMITTIME"] = dataTable.Rows[j]["PROJECTLIMITTIME"].ToString();
                    //PROJECTLIMITTIME
                    dr["LOGINID"] = a1;
                    a1++;
                    table_new.Rows.Add(dr);
                    for (int i = 0; i < table_new.Rows.Count; i++)
                    {

                        if (dataTable.Rows[j]["ID"].ToString() == table_new.Rows[i]["ID"].ToString())
                        {
                            string aavv = dataTable.Rows[j]["WELLTYPE"].ToString();
                            if (dataTable.Rows[j]["WELLTYPE"].ToString() != "")
                            {
                                table_new.Rows[i][dataTable.Rows[j]["WELLTYPE"].ToString()] = int.Parse(dataTable.Rows[j]["WELLSUM"].ToString());
                            }

                        }
                    }
                }

                //var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["ID"].ToString() == dataTable.Rows[j]["ID"].ToString());
                //if (query1.Count() > 0) { 

                //}
                //else
                //{

                //}

                //DataRow dr = table_new.NewRow();
                //dr["ID"] = dataTable.Rows[j]["ID"].ToString();
                //dr["YEAR"] = dataTable.Rows[j]["YEAR"].ToString();
                //dr["MONTH"] = dataTable.Rows[j]["MONTH"].ToString();
                //dr["SEASON"] = dataTable.Rows[j]["SEASON"].ToString();
                //dr["ROADNAME"] = dataTable.Rows[j]["ROADNAME"].ToString();
                //table_new.Rows.Add(dr);

                //for (int i = 0; i < table_new.Rows.Count; i++)
                //{
                //    if (dataTable.Rows[j]["ID"] == table_new.Rows[i]["ID"])
                //    {
                //        table_new.Rows[i][dataTable.Rows[j]["WELLTYPE"].ToString()] = int.Parse(dataTable.Rows[j]["WELLSUM"].ToString());
                //    }
                //}

            }

            for (int j = 0; j < dataTable1.Rows.Count; j++)
            {
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["ID"].ToString() == dataTable1.Rows[j]["ID"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {

                        int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());
                        string index_name = dataTable1.Rows[j]["PIPEDIAMETER"].ToString() + dataTable1.Rows[j]["PIPETYPE"].ToString();
                        index_name = index_name.Trim();
                        index_name = index_name.Replace("-", "至");
                        index_name = index_name.Replace("–", "至");
                        string jyg = dataTable1.Rows[j]["ID"].ToString();
                        if (jyg == "23")
                        {
                            string jygnum = dataTable1.Rows[j]["PIPETYPE"].ToString();
                        }
                        if (index_name != "")
                        {
                            table_new.Rows[cc - 1][index_name] = int.Parse(dataTable1.Rows[j]["PIRLSUM"].ToString());
                        }

                    }
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    dr["ID"] = dataTable1.Rows[j]["ID"].ToString();
                    dr["YEAR"] = dataTable1.Rows[j]["YEAR"].ToString();
                    dr["MONTH"] = dataTable1.Rows[j]["MONTH"].ToString();
                    dr["SEASON"] = dataTable1.Rows[j]["SEASON"].ToString();
                    dr["ROADNAME"] = dataTable1.Rows[j]["ROADNAME"].ToString();
                    dr["SUBAREA"] = dataTable.Rows[j]["SUBAREA"].ToString();
                    dr["SUBTYPE"] = dataTable.Rows[j]["SUBTYPE"].ToString();
                    dr["CREATETIME"] = dataTable.Rows[j]["CREATETIME"].ToString();
                    dr["RECORD"] = dataTable.Rows[j]["RECORD"].ToString();
                    dr["PROJECTLIMITTIME"] = dataTable.Rows[j]["PROJECTLIMITTIME"].ToString();
                    dr["LOGINID"] = a1;
                    a1++;
                    table_new.Rows.Add(dr);
                    if (table_new.Rows.Count < dataTable1.Rows.Count)
                    {
                        for (int i = 0; i < table_new.Rows.Count; i++)
                        {
                            if (dataTable1.Rows[j]["ID"] == table_new.Rows[i]["ID"])
                            {
                                string index_name = dataTable1.Rows[j]["PIPEDIAMETER"].ToString() + dataTable1.Rows[j]["PIPETYPE"].ToString();
                                index_name = index_name.Trim();
                                table_new.Rows[i][index_name] = int.Parse(dataTable1.Rows[j]["PIRLSUM"].ToString());
                            }
                        }
                    }

                }
                //DataRow dr = table_new.NewRow();
                //dr["ID"] = dataTable1.Rows[j]["ID"].ToString();
                //dr["YEAR"] = dataTable1.Rows[j]["YEAR"].ToString();
                //dr["MONTH"] = dataTable1.Rows[j]["MONTH"].ToString();
                //dr["SEASON"] = dataTable1.Rows[j]["SEASON"].ToString();
                //dr["ROADNAME"] = dataTable1.Rows[j]["ROADNAME"].ToString();
                //table_new.Rows.Add(dr);

            }

            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int abc = table_new.Columns.Count;
                for (int k = 0; k < table_new.Columns.Count; k++)
                {
                    if (table_new.Rows[i][k].ToString() == "")
                    {
                        table_new.Rows[i][k] = "0";
                    }
                }
            }
            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int WSUM = 0;
                int PSUM = 0;
                for (int j = 10; j < table_new.Columns.Count - 1; j++)
                {
                    if (j < 16)
                    {
                        PSUM += int.Parse(table_new.Rows[i][j].ToString());
                    }
                    else
                    {
                        WSUM += int.Parse(table_new.Rows[i][j].ToString());
                    }
                }
                table_new.Rows[i]["WSUM"] = WSUM;
                table_new.Rows[i]["PSUM"] = PSUM;
            }

            //return JsonConvert.SerializeObject(dataTable);
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }

    //管网总长和井室总数
    [OperationContract]
    public string Maintenance_measuring(string keyword, string starTime, string endTime)
    {
        try
        {
            string timesql = "";
            //时间筛选
            if (starTime.Trim() != "" && endTime.Trim() != "")
            {
                timesql = " and CREATETIME >= to_date('" + starTime + "','yyyy-mm-dd hh24:mi:ss') and CREATETIME <= to_date('" + endTime + "','yyyy-mm-dd hh24:mi:ss') ";
            }

            string sqlText = "";

            sqlText = "select s.areaname,SUM(t1.AMOUNT) PIPESUM from YH_PIPELINE_INFO t1 right join (select * from YH_PIPELINE_TASK where delflag = '0' " + timesql + ") t2 on t1.taskid=t2.id left join sys_areaS s on t2.areaid = s.areaid " + keyword + " group by s.areaname order by decode(s.areaname,'亭林',0,'青阳',1,'柏庐',2,'震川',3,'框一',4,'框二',5,'中维',6,7,8)";
            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;


            string sqlText1 = "";
            sqlText1 = "select s.areaname,COUNT(*) WELLSUM from YH_WELL_INFO t1 left join (select * from YH_PIPELINE_TASK  where delflag = '0' " + timesql + ") t2 on t1.taskid=t2.id left join sys_areaS s on t2.areaid = s.areaid " + keyword + " group by s.areaname order by decode(s.areaname,'亭林',0,'青阳',1,'柏庐',2,'震川',3,'框一',4,'框二',5,'中维',6,7,8)";
            System.Data.DataSet dataSet1 = oracleAccess.QueryTableInfo(sqlText1, out Ex);

            if (dataSet1 == null) return string.Empty;
            DataTable dataTable1 = dataSet1.Tables[0];
            if (dataTable1 == null) return string.Empty;

            DataTable table_new = new DataTable();

            //添加列 
            //片区
            DataColumn day_pq = new DataColumn();
            day_pq.ColumnName = "AREANAME";
            day_pq.DataType = typeof(string);
            table_new.Columns.Add(day_pq);

            //检查井总数
            DataColumn day_num_WSUM = new DataColumn();
            day_num_WSUM.ColumnName = "WELLSUM";
            day_num_WSUM.DataType = typeof(double);
            table_new.Columns.Add(day_num_WSUM);
            //管道总长度
            DataColumn day_num_PSUM = new DataColumn();
            day_num_PSUM.ColumnName = "PIPESUM";
            day_num_PSUM.DataType = typeof(double);
            table_new.Columns.Add(day_num_PSUM);

            //LOGINID 序号 
            DataColumn day_num16 = new DataColumn();
            day_num16.ColumnName = "LOGINID";
            day_num16.DataType = typeof(int);
            table_new.Columns.Add(day_num16);

            int a1 = 1;
            for (int j = 0; j < dataTable.Rows.Count; j++)
            {
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["AREANAME"].ToString() == dataTable.Rows[j]["AREANAME"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {

                        int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());
                        if ((dataTable.Rows[j]["AREANAME"].ToString()).Trim() != "")
                        {
                            if (dataTable.Rows[j]["PIPESUM"].ToString() != "")
                            {
                                table_new.Rows[cc - 1]["PIPESUM"] = double.Parse(dataTable.Rows[j]["PIPESUM"].ToString());
                            }

                        }

                    }
                }
                else
                {
                    if ((dataTable.Rows[j]["AREANAME"].ToString()).Trim() != "")
                    {

                        DataRow dr = table_new.NewRow();
                        dr["AREANAME"] = dataTable.Rows[j]["AREANAME"].ToString();
                        if ((dataTable.Rows[j]["PIPESUM"].ToString()).Trim() != "")
                        {

                            dr["PIPESUM"] = dataTable.Rows[j]["PIPESUM"].ToString();
                        }
                        else
                        {
                            dr["PIPESUM"] = 0;
                        }
                        dr["LOGINID"] = a1;
                        a1++;
                        table_new.Rows.Add(dr);
                        for (int i = 0; i < table_new.Rows.Count; i++)
                        {

                            if (dataTable.Rows[j]["AREANAME"].ToString() == table_new.Rows[i]["AREANAME"].ToString())
                            {
                                if (dataTable.Rows[j]["PIPESUM"].ToString() != "")
                                {
                                    table_new.Rows[i]["PIPESUM"] = double.Parse(dataTable.Rows[j]["PIPESUM"].ToString());
                                }

                            }
                        }

                    }
                }

                //var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["ID"].ToString() == dataTable.Rows[j]["ID"].ToString());
                //if (query1.Count() > 0) { 

                //}
                //else
                //{

                //}

                //DataRow dr = table_new.NewRow();
                //dr["ID"] = dataTable.Rows[j]["ID"].ToString();
                //dr["YEAR"] = dataTable.Rows[j]["YEAR"].ToString();
                //dr["MONTH"] = dataTable.Rows[j]["MONTH"].ToString();
                //dr["SEASON"] = dataTable.Rows[j]["SEASON"].ToString();
                //dr["ROADNAME"] = dataTable.Rows[j]["ROADNAME"].ToString();
                //table_new.Rows.Add(dr);

                //for (int i = 0; i < table_new.Rows.Count; i++)
                //{
                //    if (dataTable.Rows[j]["ID"] == table_new.Rows[i]["ID"])
                //    {
                //        table_new.Rows[i][dataTable.Rows[j]["WELLTYPE"].ToString()] = int.Parse(dataTable.Rows[j]["WELLSUM"].ToString());
                //    }
                //}

            }

            for (int j = 0; j < dataTable1.Rows.Count; j++)
            {
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["AREANAME"].ToString() == dataTable1.Rows[j]["AREANAME"].ToString());
                if (query1.Count() > 0)
                {
                    foreach (var item in query1)
                    {
                        int cc = int.Parse(item.ItemArray[item.ItemArray.Count() - 1].ToString());
                        if ((dataTable1.Rows[j]["AREANAME"].ToString()).Trim() != "")
                        {
                            if (dataTable1.Rows[j]["WELLSUM"].ToString() != "")
                            {
                                table_new.Rows[cc - 1]["WELLSUM"] = double.Parse(dataTable1.Rows[j]["WELLSUM"].ToString());
                            }
                        }

                    }
                }
                else
                {
                    if ((dataTable1.Rows[j]["AREANAME"].ToString()).Trim() != "")
                    {

                        DataRow dr = table_new.NewRow();
                        dr["AREANAME"] = dataTable1.Rows[j]["AREANAME"].ToString();
                        dr["WELLSUM"] = dataTable1.Rows[j]["WELLSUM"].ToString();
                        dr["LOGINID"] = a1;
                        a1++;
                        table_new.Rows.Add(dr);
                        if (table_new.Rows.Count < dataTable1.Rows.Count)
                        {
                            for (int i = 0; i < table_new.Rows.Count; i++)
                            {
                                if (dataTable1.Rows[j]["AREANAME"].ToString() == table_new.Rows[i]["AREANAME"].ToString())
                                {
                                    table_new.Rows[i]["WELLSUM"] = double.Parse(dataTable1.Rows[j]["WELLSUM"].ToString());
                                }
                            }
                        }

                    }

                }

            }

            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                int abc = table_new.Columns.Count;
                for (int k = 0; k < table_new.Columns.Count; k++)
                {
                    if (table_new.Rows[i][k].ToString() == "")
                    {
                        table_new.Rows[i][k] = "0";
                    }
                }
            }
            //return JsonConvert.SerializeObject(dataTable);
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }

    /// <summary>
    /// 异常数据_巡查任务
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Abnormal_data(string keyword, string starTime, string endTime, string sqlpqsx)
    {
        try
        {

            string sqlText = "select * from (select ceil((t.FINISHTIME-t.STARTTIME)*24*60*60) as DURATION," +
              "s.name as TASKNAME," +
              "s.subarea as areaname," +
              "t2.name as username," +
              "to_char(t.STARTTIME,'yyyy-mm-dd hh24:mi:ss') as STARTTIME," +
              "t.streetid as ID," +
              "to_char(t.FINISHTIME,'yyyy-mm-dd hh24:mi:ss') as FINISHTIME " +
              "from YH_TASK_STREET t " +
              "left join YH_STREETS s " +
              "on t.STREETID = s.id " +
              "left join SYS_USERINFO t2 " +
              "on t.PATROLLER = t2.ID " +
              " inner join YH_PUMP_STATION_TASK p on t.taskid=p.taskid " +
              "where t.status = '2' and t.STARTTIME is not null " + sqlpqsx + "and s.name LIKE '%" + keyword + "%'" +
              ")";

            if (starTime != "" && endTime != "")
            {
                sqlText = "select * from (select ceil((t.FINISHTIME-t.STARTTIME)*24*60*60) as DURATION," +
              "s.name as TASKNAME," +
              "s.subarea as areaname," +
              "t2.name as username," +
              "to_char(t.STARTTIME,'yyyy-mm-dd hh24:mi:ss') as STARTTIME," +
              "t.streetid as ID," +
              "to_char(t.FINISHTIME,'yyyy-mm-dd hh24:mi:ss') as FINISHTIME " +
              "from YH_TASK_STREET t " +
              "left join YH_STREETS s " +
              "on t.STREETID = s.id " +
              "left join SYS_USERINFO t2 " +
              "on t.PATROLLER = t2.ID " +
              " inner join YH_PUMP_STATION_TASK p on t.taskid=p.taskid " +
              "where t.status = '2' and t.STARTTIME is not null and t.STARTTIME >=to_date('" + starTime + " 00:00:00','yyyy/mm/dd hh24:mi:ss')and t.STARTTIME <=to_date('" + endTime + " 23:59:59','yyyy/mm/dd hh24:mi:ss')" + sqlpqsx + "and s.name LIKE '%" + keyword + "%'" +
              ")";
            }




            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;
            DataTable table_new = new DataTable();
            //table_new.AcceptChanges();
            //添加列 

            //任务名称
            DataColumn day_num = new DataColumn();
            day_num.ColumnName = "TASKNAME";
            day_num.DataType = typeof(string);
            table_new.Columns.Add(day_num);
            //片区
            DataColumn day_num_ID = new DataColumn();
            day_num_ID.ColumnName = "AREANAME";
            day_num_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID);

            //片区
            DataColumn day_num_ID1 = new DataColumn();
            day_num_ID1.ColumnName = "ID";
            day_num_ID1.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID1);



            //30秒以下
            DataColumn day_num_taskdes = new DataColumn();
            day_num_taskdes.ColumnName = "Halfminute";
            day_num_taskdes.DataType = typeof(string);
            table_new.Columns.Add(day_num_taskdes);


            //30-60秒
            DataColumn day_num1 = new DataColumn();
            day_num1.ColumnName = "Minute";
            day_num1.DataType = typeof(string);
            table_new.Columns.Add(day_num1);

            //60秒-2分钟
            DataColumn day_numpq = new DataColumn();
            day_numpq.ColumnName = "TwoMinute";
            day_numpq.DataType = typeof(string);
            table_new.Columns.Add(day_numpq);

            //2分钟以上
            DataColumn day_num2 = new DataColumn();
            day_num2.ColumnName = "MoreTwoMinute";
            day_num2.DataType = typeof(string);
            table_new.Columns.Add(day_num2);

            //合计
            DataColumn day_num3 = new DataColumn();
            day_num3.ColumnName = "eventCount";
            day_num3.DataType = typeof(string);
            table_new.Columns.Add(day_num3);

            //2分钟以上
            //DataColumn day_numpqID = new DataColumn();
            //day_numpqID.ColumnName = "TwoMinuteMore";
            //day_numpqID.DataType = typeof(string);
            //table_new.Columns.Add(day_numpqID);
            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                //string name = DateTime.Parse(dataTable.Rows[i][1].ToString()).ToString("日期MM月dd日");
                string value = dataTable.Rows[i]["TASKNAME"].ToString();
                int index = -1;
                for (int j = 0; j < table_new.Rows.Count; j++)
                {
                    if (table_new.Rows[j][0].ToString() == value)
                    {
                        index = j;
                        break;
                    }
                }
                if (index > -1)
                {
                    //table_new.Rows[index][name] = dataTable.Rows[i][2].ToString() + ',' + dataTable.Rows[i][3].ToString() + ',' + dataTable.Rows[i][4].ToString();
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    dr["TASKNAME"] = value;
                    dr["AREANAME"] = dataTable.Rows[i][2].ToString();
                    dr["ID"] = dataTable.Rows[i]["ID"].ToString();
                    table_new.Rows.Add(dr);

                }
            }



            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                var a = 0;
                var b = 0;
                var c = 0;
                var d = 0;
                for (int j = 0; j < dataTable.Rows.Count; j++)
                {
                    if (table_new.Rows[i][0].ToString() == dataTable.Rows[j][1].ToString())
                    {
                        if (Convert.ToInt32(dataTable.Rows[j][0]) <= 30)
                        {
                            a += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j][0]) > 30 && Convert.ToInt32(dataTable.Rows[j][0]) <= 60)
                        {
                            b += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j][0]) > 60 && Convert.ToInt32(dataTable.Rows[j][0]) <= 120)
                        {
                            c += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j][0]) > 120)
                        {
                            d += 1;
                        }
                    }
                }
                table_new.Rows[i][3] = a;
                table_new.Rows[i][4] = b;
                table_new.Rows[i][5] = c;
                table_new.Rows[i][6] = d;
                table_new.Rows[i][7] = a + b + c + d;

            }

            DataRow[] aa = table_new.Select("Halfminute = 0 and Minute = 0 and TwoMinute = 0 and MoreTwoMinute = 0 and eventCount = 0", "");
            foreach (DataRow item in aa)
            {
                table_new.Rows.Remove(item);
            }
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }

    /// <summary>
    /// 异常数据_养护任务
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Abnormal_data_two(string keyword, string starTime, string endTime, string sqlpqsx)
    {
        try
        {

            string sqlText = "select * from (select * from (" +
                          "select t1.ID,t1.street TASKNAME,s.areaname,'井室养护时长'type ,t2.uploadtime,t2.finishtime,ceil((t2.finishtime-t2.uploadtime)*24*60*60) as DURATION from (select * from YH_PIPELINE_TASK where delflag='0') t1  inner join YH_WELL_INFO t2 on t1.id=t2.taskid left join sys_areaS s on t1.areaid = s.areaid where t2.finishtime is not null " + sqlpqsx + " and needclean = 1))  where TASKNAME like '%" + keyword + "%'";
            if (starTime != "" && endTime != "")
            {

                sqlText = "select * from (select * from (" +
                           "select t1.ID,t1.street TASKNAME,s.areaname,'井室养护时长'type ,t2.uploadtime,t2.finishtime,ceil((t2.finishtime-t2.uploadtime)*24*60*60) as DURATION from (select * from YH_PIPELINE_TASK where delflag='0') t1  inner join YH_WELL_INFO t2 on t1.id=t2.taskid left join sys_areaS s on t1.areaid = s.areaid where t2.finishtime is not null " + sqlpqsx + " and needclean = 1))  where uploadtime >=to_date('" + starTime + " 00:00:00','yyyy/mm/dd hh24:mi:ss')and uploadtime <=to_date('" + endTime + " 23:59:59','yyyy/mm/dd hh24:mi:ss') and TASKNAME like '%" + keyword + "%'";
            }

            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;
            DataTable table_new = new DataTable();
            //table_new.AcceptChanges();
            //添加列 

            //任务名称
            DataColumn day_num = new DataColumn();
            day_num.ColumnName = "TASKNAME";
            day_num.DataType = typeof(string);
            table_new.Columns.Add(day_num);
            //片区
            DataColumn day_num_ID = new DataColumn();
            day_num_ID.ColumnName = "AREANAME";
            day_num_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID);

            //ID
            DataColumn day_num_ID1 = new DataColumn();
            day_num_ID1.ColumnName = "ID";
            day_num_ID1.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID1);

            //30秒以下
            DataColumn day_num_taskdes = new DataColumn();
            day_num_taskdes.ColumnName = "Halfminute";
            day_num_taskdes.DataType = typeof(string);
            table_new.Columns.Add(day_num_taskdes);


            //30-60秒
            DataColumn day_num1 = new DataColumn();
            day_num1.ColumnName = "Minute";
            day_num1.DataType = typeof(string);
            table_new.Columns.Add(day_num1);

            //60秒-2分钟
            DataColumn day_numpq = new DataColumn();
            day_numpq.ColumnName = "TwoMinute";
            day_numpq.DataType = typeof(string);
            table_new.Columns.Add(day_numpq);

            //2分钟以上
            DataColumn day_num2 = new DataColumn();
            day_num2.ColumnName = "MoreTwoMinute";
            day_num2.DataType = typeof(string);
            table_new.Columns.Add(day_num2);

            //合计
            DataColumn day_num3 = new DataColumn();
            day_num3.ColumnName = "eventCount";
            day_num3.DataType = typeof(string);
            table_new.Columns.Add(day_num3);

            //养护类型
            DataColumn day_numpqID = new DataColumn();
            day_numpqID.ColumnName = "TYPE";
            day_numpqID.DataType = typeof(string);
            table_new.Columns.Add(day_numpqID);
            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                //string name = DateTime.Parse(dataTable.Rows[i][1].ToString()).ToString("日期MM月dd日");
                string value = dataTable.Rows[i]["TASKNAME"].ToString();
                int index = -1;
                for (int j = 0; j < table_new.Rows.Count; j++)
                {
                    if (table_new.Rows[j][0].ToString() == value && table_new.Rows[j]["TYPE"].ToString() == dataTable.Rows[i]["TYPE"].ToString())
                    {
                        index = j;
                        break;
                    }
                }
                if (index > -1)
                {
                    //table_new.Rows[index][name] = dataTable.Rows[i][2].ToString() + ',' + dataTable.Rows[i][3].ToString() + ',' + dataTable.Rows[i][4].ToString();
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    dr["TASKNAME"] = value;
                    dr["AREANAME"] = dataTable.Rows[i]["AREANAME"].ToString();
                    dr["TYPE"] = dataTable.Rows[i]["TYPE"].ToString();
                    dr["ID"] = dataTable.Rows[i]["ID"].ToString();
                    table_new.Rows.Add(dr);
                }
            }



            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                var a = 0;
                var b = 0;
                var c = 0;
                var d = 0;
                for (int j = 0; j < dataTable.Rows.Count; j++)
                {
                    if (table_new.Rows[i]["TASKNAME"].ToString() == dataTable.Rows[j]["TASKNAME"].ToString() && table_new.Rows[i]["TYPE"].ToString() == dataTable.Rows[j]["TYPE"].ToString())
                    {
                        if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 30)
                        {
                            a += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 30 && Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 60)
                        {
                            b += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 60 && Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 120)
                        {
                            c += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 120)
                        {
                            d += 1;
                        }

                    }
                }
                table_new.Rows[i][3] = a;
                table_new.Rows[i][4] = b;
                table_new.Rows[i][5] = c;
                table_new.Rows[i][6] = d;
                table_new.Rows[i][7] = a + b + c + d;
            }

            DataRow[] aa = table_new.Select("Halfminute = 0 and Minute = 0 and TwoMinute = 0 and MoreTwoMinute = 0 and eventCount = 0", "");
            foreach (DataRow item in aa)
            {
                table_new.Rows.Remove(item);
            }
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }


    /// <summary>
    /// 异常数据_管网长度
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Abnormal_data_three(string keyword, string starTime, string endTime, string sqlpqsx)
    {
        try
        {

            string sqlText = "select t.TASKID as ID,t.TASKNAME,t.AREANAME,'管网养护长度' TYPE,t1.uploadtime,t1.finishtime,t1.amount as DURATION from YH_TASK t left join YH_PIPELINE_INFO t1 on t.TASKID =t1.taskid left join sys_areaS s on t.areaid = s.areaid where t.TYPE = '管网养护' and t.delflag = '0' and t1.finishtime is not null and t1.uploadtime is not null and t1.amount is not null and t.TASKNAME like '%" + keyword + "%'" + sqlpqsx + "";
            if (starTime != "" && endTime != "")
            {
                sqlText = "select t.TASKID as ID,t.TASKNAME,t.AREANAME,'管网养护长度' TYPE,t1.uploadtime,t1.finishtime,t1.amount as DURATION from YH_TASK t left join YH_PIPELINE_INFO t1 on t.TASKID =t1.taskid left join sys_areaS s on t.areaid = s.areaid where t.TYPE = '管网养护' and t.delflag = '0' and t1.finishtime is not null and t1.uploadtime is not null and t1.amount is not null and t1.uploadtime >=to_date('" + starTime + " 00:00:00','yyyy/mm/dd hh24:mi:ss') and t1.uploadtime <=to_date('" + endTime + " 23:59:59','yyyy/mm/dd hh24:mi:ss') and t.TASKNAME like '%" + keyword + "%'" + sqlpqsx + "";
            }

            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;
            DataTable table_new = new DataTable();
            //table_new.AcceptChanges();
            //添加列 

            //任务名称
            DataColumn day_num = new DataColumn();
            day_num.ColumnName = "TASKNAME";
            day_num.DataType = typeof(string);
            table_new.Columns.Add(day_num);
            //片区
            DataColumn day_num_ID = new DataColumn();
            day_num_ID.ColumnName = "AREANAME";
            day_num_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID);

            //ID
            DataColumn day_num_ID1 = new DataColumn();
            day_num_ID1.ColumnName = "ID";
            day_num_ID1.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID1);

            //50米以下
            DataColumn day_num_taskdes = new DataColumn();
            day_num_taskdes.ColumnName = "Halfminute";
            day_num_taskdes.DataType = typeof(string);
            table_new.Columns.Add(day_num_taskdes);


            //50-100米
            DataColumn day_num1 = new DataColumn();
            day_num1.ColumnName = "Minute";
            day_num1.DataType = typeof(string);
            table_new.Columns.Add(day_num1);

            //100-200
            DataColumn day_numpq = new DataColumn();
            day_numpq.ColumnName = "TwoMinute";
            day_numpq.DataType = typeof(string);
            table_new.Columns.Add(day_numpq);

            //200米以上
            DataColumn day_num2 = new DataColumn();
            day_num2.ColumnName = "MoreTwoMinute";
            day_num2.DataType = typeof(string);
            table_new.Columns.Add(day_num2);

            //合计
            DataColumn day_num3 = new DataColumn();
            day_num3.ColumnName = "eventCount";
            day_num3.DataType = typeof(string);
            table_new.Columns.Add(day_num3);

            //养护类型
            DataColumn day_numpqID = new DataColumn();
            day_numpqID.ColumnName = "TYPE";
            day_numpqID.DataType = typeof(string);
            table_new.Columns.Add(day_numpqID);
            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                //string name = DateTime.Parse(dataTable.Rows[i][1].ToString()).ToString("日期MM月dd日");
                string value = dataTable.Rows[i]["TASKNAME"].ToString();
                int index = -1;
                for (int j = 0; j < table_new.Rows.Count; j++)
                {
                    if (table_new.Rows[j][0].ToString() == value && table_new.Rows[j]["TYPE"].ToString() == dataTable.Rows[i]["TYPE"].ToString())
                    {
                        index = j;
                        break;
                    }
                }
                if (index > -1)
                {
                    //table_new.Rows[index][name] = dataTable.Rows[i][2].ToString() + ',' + dataTable.Rows[i][3].ToString() + ',' + dataTable.Rows[i][4].ToString();
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    dr["TASKNAME"] = value;
                    dr["AREANAME"] = dataTable.Rows[i]["AREANAME"].ToString();
                    dr["TYPE"] = dataTable.Rows[i]["TYPE"].ToString();
                    dr["ID"] = dataTable.Rows[i]["ID"].ToString();
                    table_new.Rows.Add(dr);
                }
            }



            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                var a = 0;
                var b = 0;
                var c = 0;
                var d = 0;
                for (int j = 0; j < dataTable.Rows.Count; j++)
                {
                    if (table_new.Rows[i]["TASKNAME"].ToString() == dataTable.Rows[j]["TASKNAME"].ToString() && table_new.Rows[i]["TYPE"].ToString() == dataTable.Rows[j]["TYPE"].ToString() && table_new.Rows[i]["ID"].ToString() == dataTable.Rows[j]["ID"].ToString())
                    {
                        if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 50)
                        {
                            a += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 50 && Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 100)
                        {
                            b += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 100 && Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 200)
                        {
                            c += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 200)
                        {
                            d += 1;
                        }

                    }
                }
                table_new.Rows[i][3] = a;
                table_new.Rows[i][4] = b;
                table_new.Rows[i][5] = c;
                table_new.Rows[i][6] = d;
                table_new.Rows[i][7] = a + b + c + d;
            }

            DataRow[] aa = table_new.Select("Halfminute = 0 and Minute = 0 and TwoMinute = 0 and MoreTwoMinute = 0 and eventCount = 0", "");
            foreach (DataRow item in aa)
            {
                table_new.Rows.Remove(item);
            }
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }



    /// <summary>
    /// 异常数据_管网养护时长
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Abnormal_data_four(string keyword, string starTime, string endTime, string sqlpqsx)
    {
        try
        {

            string sqlText = "select * from (select * from (" +
                          "select t.taskid as ID,t.taskname,s.areaname,'管网养护时长' type,t1.uploadtime,t1.finishtime,ceil((t1.finishtime-t1.uploadtime)*24*60*60 ) as DURATION from (select * from yh_task t2 where t2.type='管网养护'and t2.DELFLAG=0) t  inner join YH_PIPELINE_INFO t1 on t.TASKID=t1.taskid left join sys_areas s on t.areaid=s.areaid where t1.finishtime is not null " + sqlpqsx + "))  where TASKNAME like '%" + keyword + "%'";



        
            if (starTime != "" && endTime != "")
            {

                sqlText = "select * from (select * from (" +
                           "select t.taskid as ID,t.taskname,s.areaname,'管网养护时长' type,t1.uploadtime,t1.finishtime,ceil((t1.finishtime-t1.uploadtime)*24*60*60 ) as DURATION from (select * from yh_task t2 where t2.type='管网养护'and t2.DELFLAG=0) t  inner join YH_PIPELINE_INFO t1 on t.TASKID=t1.taskid left join sys_areas s on t.areaid=s.areaid where t1.finishtime is not null " + sqlpqsx + "))  where uploadtime >=to_date('" + starTime + " 00:00:00','yyyy/mm/dd hh24:mi:ss')and uploadtime <=to_date('" + endTime + " 23:59:59','yyyy/mm/dd hh24:mi:ss') and TASKNAME like '%" + keyword + "%'";
            }

            string Ex = string.Empty;
            OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
            System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqlText, out Ex);

            if (dataSet == null) return string.Empty;
            DataTable dataTable = dataSet.Tables[0];
            if (dataTable == null) return string.Empty;
            DataTable table_new = new DataTable();
            //table_new.AcceptChanges();
            //添加列 

            //任务名称
            DataColumn day_num = new DataColumn();
            day_num.ColumnName = "TASKNAME";
            day_num.DataType = typeof(string);
            table_new.Columns.Add(day_num);
            //片区
            DataColumn day_num_ID = new DataColumn();
            day_num_ID.ColumnName = "AREANAME";
            day_num_ID.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID);

            //ID
            DataColumn day_num_ID1 = new DataColumn();
            day_num_ID1.ColumnName = "ID";
            day_num_ID1.DataType = typeof(string);
            table_new.Columns.Add(day_num_ID1);

            //30秒以下
            DataColumn day_num_taskdes = new DataColumn();
            day_num_taskdes.ColumnName = "Halfminute";
            day_num_taskdes.DataType = typeof(string);
            table_new.Columns.Add(day_num_taskdes);


            //30-60秒
            DataColumn day_num1 = new DataColumn();
            day_num1.ColumnName = "Minute";
            day_num1.DataType = typeof(string);
            table_new.Columns.Add(day_num1);

            //60秒-2分钟
            DataColumn day_numpq = new DataColumn();
            day_numpq.ColumnName = "TwoMinute";
            day_numpq.DataType = typeof(string);
            table_new.Columns.Add(day_numpq);

            //2分钟以上
            DataColumn day_num2 = new DataColumn();
            day_num2.ColumnName = "MoreTwoMinute";
            day_num2.DataType = typeof(string);
            table_new.Columns.Add(day_num2);

            //合计
            DataColumn day_num3 = new DataColumn();
            day_num3.ColumnName = "eventCount";
            day_num3.DataType = typeof(string);
            table_new.Columns.Add(day_num3);

            //养护类型
            DataColumn day_numpqID = new DataColumn();
            day_numpqID.ColumnName = "TYPE";
            day_numpqID.DataType = typeof(string);
            table_new.Columns.Add(day_numpqID);
            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                //string name = DateTime.Parse(dataTable.Rows[i][1].ToString()).ToString("日期MM月dd日");
                string value = dataTable.Rows[i]["TASKNAME"].ToString();
                int index = -1;
                for (int j = 0; j < table_new.Rows.Count; j++)
                {
                    if (table_new.Rows[j][0].ToString() == value && table_new.Rows[j]["TYPE"].ToString() == dataTable.Rows[i]["TYPE"].ToString())
                    {
                        index = j;
                        break;
                    }
                }
                if (index > -1)
                {
                    //table_new.Rows[index][name] = dataTable.Rows[i][2].ToString() + ',' + dataTable.Rows[i][3].ToString() + ',' + dataTable.Rows[i][4].ToString();
                }
                else
                {
                    DataRow dr = table_new.NewRow();
                    dr["TASKNAME"] = value;
                    dr["AREANAME"] = dataTable.Rows[i]["AREANAME"].ToString();
                    dr["TYPE"] = dataTable.Rows[i]["TYPE"].ToString();
                    dr["ID"] = dataTable.Rows[i]["ID"].ToString();
                    table_new.Rows.Add(dr);
                }
            }



            for (int i = 0; i < table_new.Rows.Count; i++)
            {
                var a = 0;
                var b = 0;
                var c = 0;
                var d = 0;
                for (int j = 0; j < dataTable.Rows.Count; j++)
                {
                    if (table_new.Rows[i]["TASKNAME"].ToString() == dataTable.Rows[j]["TASKNAME"].ToString() && table_new.Rows[i]["TYPE"].ToString() == dataTable.Rows[j]["TYPE"].ToString())
                    {
                        if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 30)
                        {
                            a += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 30 && Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 60)
                        {
                            b += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 60 && Convert.ToInt32(dataTable.Rows[j]["DURATION"]) <= 120)
                        {
                            c += 1;
                        }
                        else if (Convert.ToInt32(dataTable.Rows[j]["DURATION"]) > 120)
                        {
                            d += 1;
                        }

                    }
                }
                table_new.Rows[i][3] = a;
                table_new.Rows[i][4] = b;
                table_new.Rows[i][5] = c;
                table_new.Rows[i][6] = d;
                table_new.Rows[i][7] = a + b + c + d;
            }

            DataRow[] aa = table_new.Select("Halfminute = 0 and Minute = 0 and TwoMinute = 0 and MoreTwoMinute = 0 and eventCount = 0", "");
            foreach (DataRow item in aa)
            {
                table_new.Rows.Remove(item);
            }
            string jvalue = OracleAccess.DataTableToJson(table_new);
            return jvalue;
        }
        catch (Exception ex)
        {

            return ex.Message + ex.TargetSite;
        }
    }



    /// <summary>
    /// 快捷添加路线或道路
    /// </summary>
    /// <param name="roname">多条道路</param>
    /// <param name="areaname">片区名称</param>
    /// <param name="type">建筑类型</param>
    /// <param name="roadname">路线名称</param>
    /// <param name="roadbool">生不生成路线</param>
    /// <returns></returns>
    [OperationContract]
    public int RouteGenerate(string roname, string areaname, string areaid, string type, string roadname, string roadbool)
    {
        int count = 0;
        if (roname != "" && areaname != "" && type != "")
        {
            if (int.Parse(roadbool) == 1)
            {
                //生成路线
                if (roadname != "")
                {
                    //count = Maintenance_area(roname, areaname, type, areaid);
                    count = Maintenance_rouad(roname, areaname, type, roadname);
                }
                else
                {
                    count = 0;
                }
            }
            else
            {
                //不生成路线
                count = Maintenance_area(roname, areaname, type, areaid);
            }
        }
        else
        {
            count = 0;
        }


        return count;
    }

    //道路或小区
    /// <summary>
    /// 添加道路
    /// </summary>
    /// <param name="roname">道路总集</param>
    /// <param name="areaname">片区名称</param>
    /// <param name="type">建筑类型</param>
    /// <returns></returns>
    public int Maintenance_area(string roname, string areaname, string type, string areaid)
    {
        OracleAccess oracleAccess = new OracleAccess("OrclConnStr");
        string[] strarr = roname.Split('、');
        int count = 0;
        for (int i = 0; i < strarr.Length; i++)
        {

            if (strarr[i] != "")
            {
                string sqltxt1 = "select * from YH_STREETS where name ='" + strarr[i] + "' and  SUBAREA='" + areaname.Trim() + "'";
                string Ex = string.Empty;
                System.Data.DataSet dataSet = oracleAccess.QueryTableInfo(sqltxt1, out Ex);
                DataTable datatable = dataSet.Tables[0];
                if (datatable.Rows.Count == 0)
                {
                    string sqltxt = "insert into YH_STREETS (ID,NAME,CREATETIME,SUBAREA,CONSTYPE,AREAID) values((select nvl(MAX(ID)+1,0) ID from YH_STREETS ),'" + strarr[i] + "',(select sysdate from dual),'" + areaname + "','" + type + "','" + areaid.Trim() + "') ";
                    count = oracleAccess.UpdateTableBySQL(sqltxt, out Ex);
                }
            }

        }
        return count;
    }

    //生成路线
    /// <summary>
    /// 生成路线
    /// </summary>
    /// <param name="roname">道路名称总集</param>
    /// <param name="areaname">片区名称</param>
    /// <param name="type">建筑类型</param>
    /// <param name="roadname">路线名称</param>
    /// <returns></returns>
    public int Maintenance_rouad(string roname, string areaname, string type, string roadname)
    {
        string[] strarr = roname.Split('、');
        string Ex = string.Empty;
        OracleAccess oracleAccess = new OracleAccess("OrclConnStr");

        string sqltxt1 = "select * from SYS_AREAS where AREANAME='" + (areaname).Trim() + "'";
        DataTable datatable = oracleAccess.QueryTableInfo(sqltxt1, out Ex).Tables[0];
        string areaid = datatable.Rows[0]["AREAID"].ToString();
        string namesql = "";
        for (int i = 0; i < strarr.Length; i++)
        {
            if (strarr[i] != "")
            {
                if (i == 0)
                {
                    namesql += "  NAME='" + strarr[i] + "'";
                }
                else
                {
                    namesql += " or NAME='" + strarr[i] + "'";
                }
            }
        }
        string sqltxt = "select * from YH_STREETS where SUBAREA='" + (areaname).Trim() + "' and (" + namesql + ")";
        //DataTable datatable1 = OracleAccess.QueryTableInfo(sqltxt).Tables[0];

        sqltxt = "  insert into YH_PATROL_ROUTE (ID,ROUTENAME,INVOLVEDROAD,Createtime,AREAID) " +
            " select (select nvl(max(ID)+1,0) ID from YH_PATROL_ROUTE ) as ID,'" + (roadname).Trim() + "' as ROUTENAME,t.ID as INVOLVEDROAD,(select sysdate from dual) as Createtime,'" + areaid + "' as AREAID " +
            " from YH_STREETS t where SUBAREA='" + (areaname).Trim() + "'and  ( " + namesql + ")";

        int count = count = oracleAccess.UpdateTableBySQL(sqltxt, out Ex);
        return count;
    }

    [OperationContract]
    public string DownloadPic(string TaskID)
    {
        DownloadPic pic = new DownloadPic();
        string pathrar = pic.GetImg(TaskID);
        pathrar = pathrar.Substring(pathrar.LastIndexOf('\\') + 1);
        return pathrar;
    }

    //[OperationContract]
    //public string get_DMAinfo(string NAME)
    //{
    //    string sqltxt = "";
    //    sqltxt = "select DMANAME as DMA名称,MAINSLENGTH as 管线长度,USERSNUMBER as 用户数量,POPULATION as 人口数量,DAILYSUPPLY as 日供水量,DAILYCONSUMPTION as 日用水量,NIGHTCONSUMPTION as 夜间用水量,AVPRESSURE*1000 as 平均压力 from LKG_DMAINFO t where t.dmaname='" + NAME + "'";
    //    return QueryTableBySQL(sqltxt);
    //}
    [OperationContract]
    public string get_isUser(string NAME, string PASSWORD)
    {
        string sqltxt = "";
        sqltxt = "select * from SYS_USERINFO t where t.username ='" + NAME + "' and t.password ='" + PASSWORD + "'";
        //string u = QueryTableBySQL(sqltxt);
        //if (u == "")
        //    return "false";
        //else
        return "true";

    }
    //[OperationContract]
    //public string get_DMA_Pipeinfo(string NAME)
    //{
    //    string sqltxt = "";
    //    sqltxt = "select t.material as name,t.length as y from LKG_DMAPIPEMATERIAL t,lkg_dMAinfo b where b.dmaid =t.dmaid and  b.dmaname='" + NAME + "'";
    //    return QueryTableBySQL(sqltxt);
    //}
    [OperationContract]
    public string set_UpUser(string NAME, string PASSWORD)
    {
        string sqltxt = "";
        sqltxt = "update SYS_USERINFO set  password ='" + PASSWORD + "' where username ='" + NAME + "'";
        if (UpdateTableBySQL(sqltxt) == 0)
            return "false";
        else
            return "true";
    }

    [OperationContract]
    public string get_F_QX(string FID, string starTime, string endTime, string Type, string NAME)
    {
        string id = FID;
        if (id.Length == 1)
        { id = "00" + id; }
        if (id.Length == 2)
        { id = "0" + id; }
        string dmajson = "[";
        if (Type == "1")
        {
            string sql = "select t.recordtime,t.recordvalue from FLOW_DATA_" + id + " t "
            + " where t.recordtime >= to_date('" + starTime + "','yyyy-mm-dd  hh24:mi:ss') and t.recordtime < to_date('" + endTime + "','yyyy-mm-dd  hh24:mi:ss') order by recordtime asc";
            string rec = OracleAccess.DataTableToJson(get_sp_table(QueryTableBySQL_t(sql)));
            if (rec != "")
            {
                rec = rec.Remove(rec.Length - 1, 1);
                rec = rec.Remove(0, 1);
            }
            dmajson = dmajson + "{'name':'" + NAME + "','type':'METER','DATA':'" + rec + "'},";
        }
        else if (Type == "6")
        {
            string sql = "select t.recordtime,(t.PRESSUREVALUE)*1000 as recordvalue from FLOW_DATA_" + id + " t "
            + " where t.recordtime >= to_date('" + starTime + "','yyyy-mm-dd  hh24:mi:ss') and t.recordtime < to_date('" + endTime + "','yyyy-mm-dd  hh24:mi:ss') order by recordtime asc";
            string rec = OracleAccess.DataTableToJson(get_sp_table(QueryTableBySQL_t(sql)));
            if (rec != "")
            {
                rec = rec.Remove(rec.Length - 1, 1);
                rec = rec.Remove(0, 1);
            }
            dmajson = dmajson + "{'name':'" + NAME + "','type':'METER','DATA':'" + rec + "'},";
        }
        if (Type == "100")
        {
            string sql = "select t.READtime,(t.VALUE)*1000 as recordvalue from CZ_SBDATA_" + id + " t "
            + " where OID='YL' t.READtime >= to_date('" + starTime + "','yyyy-mm-dd  hh24:mi:ss') and t.READtime < to_date('" + endTime + "','yyyy-mm-dd  hh24:mi:ss') order by READtime asc";
            string rec = OracleAccess.DataTableToJson(get_sp_table(QueryTableBySQL_t(sql)));
            if (rec != "")
            {
                rec = rec.Remove(rec.Length - 1, 1);
                rec = rec.Remove(0, 1);
            }
            dmajson = dmajson + "{'name':'" + NAME + "','type':'METER','DATA':'" + rec + "'},";
        }
        dmajson.Remove(dmajson.Length - 1, 1);
        dmajson = dmajson + "]";
        return dmajson.ToUpper();
    }

    [OperationContract]
    public string get_KHB_SZQX(string KHBID, string starTime, string endTime, string Type, string NAME)
    {
        string id = KHBID;
        string dmajson = "[";
        string tablename = "CZ_SZDTIMEDATA";



        string fieldname = Type;
        //if (Type == "10")
        //{ fieldname = "PH"; }
        //if (Type == "11")
        //{ fieldname = "YLZ"; }
        //if (Type == "12")
        //{ fieldname = "GNWD"; }
        //if (Type == "13")
        //{ fieldname = "GNSD"; }
        //if (Type == "14")
        //{ fieldname = "PH"; }
        string wheretxt = " where t.oid ='" + id + "' and t." + fieldname + " is not null and  t.READTIME >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.READTIME < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by READTIME asc";
        string sql = "select t.READTIME as datetime,(t." + fieldname + ") as recordvalue from " + tablename + " t "
          + wheretxt;


        DataTable datat = QueryTableBySQL_t(sql);
        if (datat == null)
        { return ""; }
        string rec = OracleAccess.DataTableToJson(get_mc_table(datat));
        if (rec != "")
        {
            rec = rec.Remove(rec.Length - 1, 1);
            rec = rec.Remove(0, 1);
        }
        dmajson = dmajson + "{'name':'" + NAME + "','type':'METER','DATA':'" + rec + "'},";



        dmajson.Remove(dmajson.Length - 1, 1);
        dmajson = dmajson + "]";
        return dmajson.ToUpper();
    }

    [OperationContract]
    public string get_KHB_YSQX(string KHBID, string starTime, string endTime, string Type, string NAME)
    {
        string id = KHBID;
        string dmajson = "[";
        string tablename = "CZ_SCADATIMEDATA";

        if (id == "9001" || id == "9002" || id == "9003" || id == "9004" || id == "9005" || id == "9006" || id == "9007" || id == "9008")
        { tablename = "CZ_SCADATIMEDATA"; }
        else
        { tablename = "CZ_TIMEDATA_" + id; }
        if (Type == "6" || Type == "10" || Type == "11" || Type == "12" || Type == "13" || Type == "14")
        {
            string wheretxt = "";
            string sql = "select t.RECORDTIME as datetime,(t.VALUE) as recordvalue from " + tablename + " t "
            + wheretxt;
            if (id == "9001" || id == "9002" || id == "9003" || id == "9004" || id == "9005" || id == "9006" || id == "9007" || id == "9008")
            {
                string fieldname = "GDYL";
                if (Type == "10")
                { fieldname = "PH"; }
                if (Type == "11")
                { fieldname = "YLZ"; }
                if (Type == "12")
                { fieldname = "GNWD"; }
                if (Type == "13")
                { fieldname = "GNSD"; }
                if (Type == "14")
                { fieldname = "PH"; }
                wheretxt = " where t.oid ='" + id + "' and t." + fieldname + " is not null and  t.READTIME >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.READTIME < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by READTIME asc";
                sql = "select t.READTIME as datetime,(t." + fieldname + ") as recordvalue from " + tablename + " t "
                + wheretxt;
            }
            else
            {
                wheretxt = " where t.value is not null and  t.RECORDTIME >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.RECORDTIME < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by RECORDTIME asc";
                sql = "select t.RECORDTIME as datetime,(t.VALUE) as recordvalue from " + tablename + " t "
                + wheretxt;
            }

            DataTable datat = QueryTableBySQL_t(sql);
            if (datat == null)
            { return ""; }
            string rec = OracleAccess.DataTableToJson(get_mc_table(datat));
            if (rec != "")
            {
                rec = rec.Remove(rec.Length - 1, 1);
                rec = rec.Remove(0, 1);
            }
            dmajson = dmajson + "{'name':'" + NAME + "','type':'METER','DATA':'" + rec + "'},";

        }
        if (int.Parse(Type) / 100 == 1)
        {
            string sql = "select t.READtime AS DATETIME,(t.VALUE*1000) as recordvalue from CZ_SBDATA_" + id + " t "
            + " where OID='YL' AND t.READtime >= to_date('" + starTime + "','yyyy-mm-dd  hh24:mi:ss') and t.READtime < to_date('" + endTime + "','yyyy-mm-dd  hh24:mi:ss') order by READtime asc";
            if (Type == "101")
            {
                sql = "select t.READtime AS DATETIME,(t.VALUE*1000) as recordvalue from CZ_SBDATA_" + id + " t "
            + " where OID='LL' AND t.READtime >= to_date('" + starTime + "','yyyy-mm-dd  hh24:mi:ss') and t.READtime < to_date('" + endTime + "','yyyy-mm-dd  hh24:mi:ss') order by READtime asc";
            }
            DataTable datat = QueryTableBySQL_t(sql);
            if (datat == null)
            { return ""; }
            string rec = OracleAccess.DataTableToJson(get_mc_table(datat));
            if (rec != "")
            {
                rec = rec.Remove(rec.Length - 1, 1);
                rec = rec.Remove(0, 1);
            }
            dmajson = dmajson + "{'name':'" + NAME + "','type':'METER','DATA':'" + rec + "'},";
        }
        else
        {
            string wheretxt = "";
            string sql = "select t.RECORDTIME as datetime,(t.VALUE*1000) as totalvalue from " + tablename + " t "
            + wheretxt;
            if (id == "9001" || id == "9002" || id == "9003" || id == "9004" || id == "9005" || id == "9006" || id == "9007" || id == "9008")
            {
                string fieldname = "GDYL";
                if (Type.Contains("12_"))
                {
                    fieldname = "GNWD";
                    Type = Type.Split('_')[1];
                }
                if (Type.Contains("13_"))
                {
                    fieldname = "GNSD";
                    Type = Type.Split('_')[1];
                }
                if (Type.Contains("14_"))
                {
                    fieldname = "PH";
                    Type = Type.Split('_')[1];
                }
                wheretxt = " where t.oid ='" + id + "' and t." + fieldname + "  is not null and  t.READTIME >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.READTIME < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by READTIME asc";
                sql = "select t.READTIME as datetime,(t." + fieldname + " ) as totalvalue from " + tablename + " t "
                + wheretxt;
            }
            else
            {
                wheretxt = " where t.value is not null and  t.RECORDTIME >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.RECORDTIME < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by RECORDTIME asc";
                sql = "select t.RECORDTIME as datetime,(t.VALUE*1000) as totalvalue from " + tablename + " t "
                + wheretxt;
            }
            DataTable daTable = QueryTableBySQL_t(sql);
            if (daTable == null)
            { return ""; }

            daTable = get_Table(daTable, Type);
            string rec = OracleAccess.DataTableToJson(get_mc_table(daTable));
            if (rec != "")
            {

                rec = rec.Remove(rec.Length - 1, 1);
                rec = rec.Remove(0, 1);
            }
            dmajson = dmajson + "{'name':'" + NAME + "','type':'METER','DATA':'" + rec + "'},";
        }
        dmajson.Remove(dmajson.Length - 1, 1);
        dmajson = dmajson + "]";
        return dmajson.ToUpper();
    }

    public DataTable get_sp_table(DataTable dataTable)
    {
        for (int i = dataTable.Rows.Count - 1; i >= 0; i--)
        {
            if (i % 6 != 0)
            { dataTable.Rows.RemoveAt(i); }

        }
        return dataTable;
    }

    public DataTable get_mc_table(DataTable dataTable)
    {
        for (int i = dataTable.Rows.Count - 1; i >= 1; i--)
        {
            try
            {
                double sta = double.Parse(dataTable.Rows[i][1].ToString());
                double end = double.Parse(dataTable.Rows[i - 1][1].ToString());
                double xz = sta - end;
                if (xz > 80)
                {
                    int a = 0;
                    for (int j = i - 1; j >= 0; j--)
                    {
                        a++;
                        if (Math.Abs(end - double.Parse(dataTable.Rows[j][1].ToString())) > 80)
                        {
                            dataTable.Rows.RemoveAt(i - 1);
                            break;
                        }
                        if (a > 6)
                        { break; }
                    }
                }
            }
            catch { continue; }
        }
        return dataTable;
    }

    [OperationContract]
    public string get_DMA_YSQX(string DMAID, string starTime, string endTime, string Type, string NAME)
    {
        string sqltxt = "";
        sqltxt = "select * from REMOTE_METERINFO t WHERE DMAID='" + DMAID + "'";
        DataTable dTable = QueryTableBySQL_t(sqltxt);
        string dmajson = "[";
        List<DataTable> list = new List<DataTable>();
        for (int i = 0; i < dTable.Rows.Count; i++)
        {
            string id = dTable.Rows[i]["OID"].ToString();
            if (id.Length == 1)
            { id = "00" + id; }
            if (id.Length == 2)
            { id = "0" + id; }
            if (Type == "1")
            {
                string sql = "select t.recordtime,t.recordvalue from REMOTE_DATA_" + id + " t "
                + " where t.recordtime >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.recordtime < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by recordtime asc";
                string rec = OracleAccess.DataTableToJson(get_sp_table(QueryTableBySQL_t(sql)));
                if (rec != "")
                {
                    rec = rec.Remove(rec.Length - 1, 1);
                    rec = rec.Remove(0, 1);
                }
                dmajson = dmajson + "{'name':'" + dTable.Rows[i]["NAME"].ToString() + "','type':'METER','DATA':'" + rec + "'},";
            }
            else if (Type == "4")
            {
                string sql = "select t.recordtime,t.totalvalue as recordvalue from REMOTE_DATA_" + id + " t "
                + " where t.recordtime >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.recordtime < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by recordtime asc";
                string rec = OracleAccess.DataTableToJson(get_sp_table(QueryTableBySQL_t(sql)));
                if (rec != "")
                {
                    rec = rec.Remove(rec.Length - 1, 1);
                    rec = rec.Remove(0, 1);
                }
                dmajson = dmajson + "{'name':'" + dTable.Rows[i]["NAME"].ToString() + "','type':'METER','DATA':'" + rec + "'},";
            }
            else if (Type == "6")
            {
                string sql = "select t.recordtime,(t.PRESSUREVALUE)*1000 as recordvalue from REMOTE_DATA_" + id + " t "
                + " where t.recordtime >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.recordtime < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by recordtime asc";
                string rec = OracleAccess.DataTableToJson(get_sp_table(QueryTableBySQL_t(sql)));
                if (rec != "")
                {
                    rec = rec.Remove(rec.Length - 1, 1);
                    rec = rec.Remove(0, 1);
                }
                dmajson = dmajson + "{'name':'" + dTable.Rows[i]["NAME"].ToString() + "','type':'METER','DATA':'" + rec + "'},";
            }
            else
            {
                string sql = "select t.recordtime,t.totalvalue from REMOTE_DATA_" + id + " t "
                + " where t.recordtime >= to_date('" + starTime + " 00:00:00','yyyy-mm-dd  hh24:mi:ss') and t.recordtime < to_date('" + endTime + " 23:59:59','yyyy-mm-dd  hh24:mi:ss') order by recordtime asc";
                DataTable daTable = QueryTableBySQL_t(sql);
                daTable = get_Table(daTable, Type);
                string rec = OracleAccess.DataTableToJson(daTable);
                if (rec != "")
                {
                    rec = rec.Remove(rec.Length - 1, 1);
                    rec = rec.Remove(0, 1);
                }
                dmajson = dmajson + "{'name':'" + dTable.Rows[i]["NAME"].ToString() + "','type':'METER','DATA':'" + rec + "'},";
                list.Add(daTable);
            }
        }
        if (list.Count > 1)
        {
            string rec = OracleAccess.DataTableToJson(get_dmaTable(list));
            if (rec != "")
            {
                rec = rec.Remove(rec.Length - 1, 1);
                rec = rec.Remove(0, 1);
            }
            dmajson = dmajson + "{'name':'" + NAME + "','type':'DMA','DATA':'" + rec + "'},";
        }
        if (list.Count == 1)
        {
            string rec = OracleAccess.DataTableToJson(list[0]);
            if (rec != "")
            {
                rec = rec.Remove(rec.Length - 1, 1);
                rec = rec.Remove(0, 1);
            }
            dmajson = dmajson + "{'name':'" + NAME + "','type':'METER','DATA':'" + rec + "'},";
        }
        dmajson.Remove(dmajson.Length - 1, 1);
        dmajson = dmajson + "]";
        return dmajson.ToUpper();
    }

    public DataTable get_dmaTable(List<DataTable> list)
    {
        DataTable dat = new DataTable();
        DataColumn daC = new DataColumn();
        daC.ColumnName = "DATETIME";
        daC.DataType = typeof(DateTime);
        dat.Columns.Add(daC);
        DataColumn daC1 = new DataColumn();
        daC1.ColumnName = "recordvalue";
        daC1.DataType = typeof(string);
        dat.Columns.Add(daC1);
        if (list[0].Rows.Count > 0)
        {
            DateTime stime = DateTime.Parse(list[0].Rows[0]["recordtime"].ToString());
            DateTime etime = DateTime.Parse(list[0].Rows[list[0].Rows.Count - 1]["recordtime"].ToString());
            for (int i = 0; i < list.Count; i++)
            {
                DateTime time = DateTime.Parse(list[i].Rows[0]["recordtime"].ToString());
                DateTime time_ = DateTime.Parse(list[i].Rows[list[i].Rows.Count - 1]["recordtime"].ToString());
                if (time > stime)
                { stime = time; }
                if (time_ < etime)
                { etime = time_; }
            }
            for (int i = 0; i < list.Count; i++)
            {
                int sint = 0;
                for (int j = 0; j < list[i].Rows.Count; j++)
                {
                    if (i == 0)
                    {
                        DateTime time = DateTime.Parse(list[i].Rows[j]["recordtime"].ToString());
                        if (time >= stime && time <= etime)
                        {
                            DataRow dr = dat.NewRow();
                            dr[0] = time;
                            dr[1] = list[i].Rows[j]["recordvalue"].ToString();
                            dat.Rows.Add(dr);
                        }
                    }
                    else
                    {
                        DateTime time = DateTime.Parse(list[i].Rows[j]["recordtime"].ToString());
                        if (time == stime)
                        { sint = j; }
                        if (time >= stime && time <= etime)
                        {
                            dat.Rows[j - sint][1] = double.Parse(dat.Rows[j - sint][1].ToString()) + double.Parse(list[i].Rows[j]["recordvalue"].ToString());
                        }
                    }
                }
            }
        }
        return dat;
    }


    public DataTable get_Table(DataTable datable, string type)
    {
        DataTable dat = new DataTable();
        DataColumn daC = new DataColumn();
        daC.ColumnName = "DATETIME";
        daC.DataType = typeof(DateTime);
        dat.Columns.Add(daC);
        DataColumn daC1 = new DataColumn();
        daC1.ColumnName = "recordvalue";
        daC1.DataType = typeof(string);
        dat.Columns.Add(daC1);
        DateTime stime = DateTime.Now;
        double stotal = 0;
        int type_mi = 15;
        if (type == "2")
        { type_mi = 15; }
        if (type == "3")
        { type_mi = 60; }
        if (type == "5")
        { type_mi = 5; }
        for (int i = 0; i < datable.Rows.Count; i++)
        {
            DateTime date = DateTime.Parse(datable.Rows[i]["DATETIME"].ToString());
            int mi = date.Minute;
            if (i == 0)
            {
                stime = date;
                stotal = double.Parse(datable.Rows[i]["totalvalue"].ToString());
            }
            TimeSpan ts = date - stime;
            if (ts.TotalMinutes >= type_mi)
            {
                double total = double.Parse(datable.Rows[i]["totalvalue"].ToString());
                //total = 60 * 60 * total / ts.TotalSeconds;
                int s = int.Parse(Math.Round(ts.TotalMinutes).ToString()) / type_mi;
                DateTime de = stime;
                for (int j = 0; j < s; j++)
                {
                    DataRow dr = dat.NewRow();
                    de = DateTime.Parse(stime.Year + "/" + stime.Month + "/" + stime.Day + " " + stime.Hour + ":" + stime.Minute + ":00").AddMinutes(type_mi - stime.Minute % type_mi + type_mi * j);
                    dr[1] = Math.Round(total, 2);
                    dr[0] = de;
                    dat.Rows.Add(dr);
                }
                stotal = stotal + ts.TotalSeconds * total / (60 * 60);
                stime = de;

            }
        }
        return dat;
    }


    [OperationContract]
    public string set_Meter_DMAID(string CUSTOMERID, string DMAID)
    {
        string[] id = CUSTOMERID.Split(',');
        for (int i = 0; i < id.Length; i++)
        {
            string sqltxt = "";
            sqltxt = "update BILLING_METERINFO set  DMAID ='" + DMAID + "' where CUSTOMERID ='" + id[i] + "'";
            UpdateTableBySQL(sqltxt);
        }
        return "";
    }
    //[OperationContract]
    //public string get_FlowMerterInfo()
    //{
    //    return get_Info("FlowMeter");
    //}
    //[OperationContract]
    //public string get_YLDInfo()
    //{
    //    return get_Info("Pressure");
    //}
    //[OperationContract]
    //public string get_SZDInfo()
    //{
    //    return get_Info("FlowRate");
    //}
    //[OperationContract]
    //public string get_ColumnsName(string name)
    //{
    //    string sqltxt = "select  distinct  a.COLUMN_NAME as name, b.comments as alias from user_tab_columns a,user_col_comments b where a.TABLE_NAME=b.table_name and a.column_name=b.column_name and a.table_name=upper('" + name + "')";
    //    return QueryTableBySQL(sqltxt);
    //}
    //[OperationContract]
    //public string get_Info(string TYPE)
    //{
    //    string sqltxt = "select t.OID,d.recordtime,d.recordvalue,d.totalvalue,d.PRESSUREVALUE,d.status,t.* from FLOW_METERINFO t,FLOW_DATA_TIME d,FLOW_DATA_REL S where  t.x is not null and d.OID =t.OID AND S.OID =T.OID AND S.DATATYPE LIKE '%" + TYPE + "%'";
    //    return QueryTableBySQL(sqltxt);
    //}


    /// 计划管理--巡查计划--xcjh_delete_Route_oper
    [OperationContract]
    public int xcjh_delete_Route_oper(string ronameID)
    {
        //sql语句
        string sqltext = "delete from YH_PATROL_ROUTE where ID='" + ronameID + "'";

        //获取key值
        string key = DataInfo.Key_encryption("YH_PATROL_ROUTE");

        //获取数据
        int data = DataInfo.UpdateTableBySQL_test(sqltext, key);
        return data;
    }


}
