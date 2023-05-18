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
// 注意: 使用“重构”菜单上的“重命名”命令，可以同时更改代码和配置文件中的接口名“ITest”。

//[ServiceContract]
public class Logical
{
    /// <summary>

    /// 登录
    /// </summary>
    /// <param name="uid">账号</param>
    /// <param name="pwd">密码</param>
    /// <returns></returns>
    [OperationContract]
    public string LoginF(string uid, string pwd)
    {
        //sql语句
        string sqltext = "select * from SYS_USERINFO where USERNAME='" + uid + "' and PASSWORD='" + pwd + "'";

        //获取key值
        string key = DataInfo.Key_encryption("SYS_USERINFO");

        //获取数据
        string data = DataInfo.QueryTableBySQL(sqltext, key);
        return data;
    }

    /// <summary>
    /// 获取登录信息
    /// </summary>
    /// <param name="uid">账号</param>
    /// <param name="pwd">密码</param>
    /// <returns></returns>
    [OperationContract]
    public string getUserinfo(string uid, string pwd)
    {
        //sql语句
        string sqltext = "select * from SYS_USERINFO t1 left join SYS_AREAS t2 on t1.area=t2.areaid where USERNAME='" + uid + "' and PASSWORD='" + pwd + "'";

        //获取key值
        string key = DataInfo.Key_encryption("SYS_USERINFO");

        //获取数据
        string data = DataInfo.QueryTableBySQL(sqltext, key);
        return data;
    }

    
    //// 新建工程
    [OperationContract]
    public int projectInsert2(string gcbh,string gcmc,string gclb,string gcys,string gcdz,string userName,string gcbz,string jjd,string pqId,string gsId)
    {
 
        //sql语句
        string sqltxt = "insert into GC_ENGINEERING_INFO (ID,CREATETIME,EID,ENAME,CATEGORY,BUDGET,ADDRESS,CREATEPERSON,REMARK,URGENCY,AREAID,COMPANYID) values((select nvl(max(ID),0)+1 as ID from GC_ENGINEERING_INFO),(select sysdate from dual),'"+gcbh+"','"+gcmc+"','"+gclb+"','"+gcys+"','"+gcdz+"','"+userName+"','"+gcbz+"','"+jjd+"','"+pqId+"','"+gsId+"')";


        //获取key值
        string key = DataInfo.Key_encryption("GC_ENGINEERING_INFO");

        //获取数据
        int data = DataInfo.UpdateTableBySQL(sqltxt,key);
        return data;
    }


   //// 获取当前登录信息
    [OperationContract]
    public string userLogin(string userName)
    {
        //sql语句
        string sqltxt  ="select * from sys_userinfo where username = '"+userName+"'";

        //获取key值
        string key = DataInfo.Key_encryption("sys_userinfo");

        //获取数据
        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }
    
    //// 获取所有的公司
    [OperationContract]
    public string allCompanys(string UserID)
    {
        //sql语句
        string sqltxt  ="select COMPANYID,COMPANYNAME from sys_company_tree where PARENTCOMPANYID='"+UserID+"'  order by COMPANYID ";
        //获取key值
        string key = DataInfo.Key_encryption("sys_company_tree");
        //获取数据
        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }
    //// 获取所有片区
    [OperationContract]
    public string allAreas(string UserID)
    {
        //sql语句
        string sqltext = "select AREAID,AREANAME from sys_area_tree where PARENTAREAID='" + UserID + "' order by AREAID ";
        sqltext = @"select * from (
select PARENTAREAID,PARENTAREANAME,AREAID,AREANAME from sys_area_tree where AREAID not in (select AREAID from sys_area_tree t where length(to_char(parentareaid)) = 3) and length(to_char(parentareaid)) = 2
union all
select PARENTAREAID,PARENTAREANAME,AREAID,AREANAME from sys_area_tree t where length(to_char(parentareaid)) = 3 and areaid not in (select areaid from sys_area_tree t where parentareaid = 1029)
union all
select * from sys_area_tree t where parentareaid = 1029) 
where AREAID in (select AREAID from sys_area_tree where PARENTAREAID='" + UserID + "' ) order by PARENTAREAID,AREAID";
        //获取key值
        string key = DataInfo.Key_encryption("sys_area_tree");
        //获取数据
        string data = DataInfo.QueryTableBySQL(sqltext, key);
        return data;
    }
    

    //// 获取所有状态
    [OperationContract]
    public string statusNode()
    {
        //sql语句
        string sqltxt  ="select * from GC_STATUS_NODE";

        //获取key值
        string key = DataInfo.Key_encryption("GC_STATUS_NODE");

        //获取数据
        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }


    
    //// 工程管理数据
    [OperationContract]
    public string Engineer_info(string searchInput,string gczt,string gcTime1,string gcTime2,string gcgs,string gcpq)
    {
         
        // 关键字
        string gcglgjz = "";
        if (searchInput == "")
        {
            gcglgjz = "";
        }
        else
        {
            gcglgjz = "and (ENAME like '%" + searchInput + "%' or EID like '%" + searchInput + "%')";
        }
        // 状态
        string gcglzt = "";
        if(gczt == "全部")
        {
            gcglzt= "";
        }
        else
        {
            gcglzt = "and STATUS = '"+ gczt +"'";
        }
        // 时间
        string gcglshijian = "";
        if (gcTime1 == "")
        {
            gcglshijian = "";
        }
        else
        {
            gcglshijian ="and( CREATETIME>=to_date('"+gcTime1 +"','yyyy/mm/dd') and CREATETIME<=to_date('"+gcTime2 +"','yyyy/mm/dd') ) ";
        }

        //sql语句
        // string sqltxt = "select * from GC_ENGINEERING_INFO WHERE 1=1 "+gcglgjz+""+gcglzt+""+gcglshijian+"order by CREATETIME desc";
        string sqltxt  ="select e.*,c1.companyname as constructionunitname,c2.companyname as supervisoryunitname,c3.companyname as designunitname,s.nodename,a.companyname,b.areaname from GC_ENGINEERING_INFO e left join sys_companies2 c1 on e.constructionunit=c1.companyid left join sys_companies2 c2 on e.supervisoryunit=c2.companyid left join sys_companies2 c3 on e.designunit=c3.companyid left join GC_STATUS_NODE s on e.status=s.nodeid left join SYS_COMPANIES2 a on e.companyid = a.companyid left join SYS_AREAS b on e.areaid = b.areaid WHERE 1=1 and e.COMPANYID in("+gcgs+") and e.AREAID in ("+gcpq+") "+gcglgjz+""+gcglzt+""+gcglshijian+" order by e.CREATETIME desc";         


        //获取key值
        string key = DataInfo.Key_encryption("GC_ENGINEERING_INFO");
        //获取数据

        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }
   
     //// 删除
    [OperationContract]
    public int deleteData(string gcbh)
    {
        //sql语句
        string sqltxt = "DELETE FROM GC_ENGINEERING_INFO WHERE EID ='"+gcbh+"'";

        //获取key值
        string key = DataInfo.Key_encryption("GC_ENGINEERING_INFO");

        //获取数据
        int data = DataInfo.UpdateTableBySQL(sqltxt, key);
        return data;
    }

    //// 更改状态
   [OperationContract]
    public int changeProject(string gczt,string gcbh)
    {
        //sql语句
        string sqltxt = "update GC_ENGINEERING_INFO SET STATE = '"+gczt+"' where EID = '"+gcbh+"'";

        //获取key值
        string key = DataInfo.Key_encryption("GC_ENGINEERING_INFO");

        //获取数据
        int data = DataInfo.UpdateTableBySQL(sqltxt,key);
        return data;
    }

    //// 插入审核数据
    [OperationContract]
    public int InsertAuditData(string gcbh,string opinion,string reviewPerson,string reviewTime,string review,string type)
    {
        //sql语句
        string sqltxt = "insert into GC_REVIEW_INFO(EID,OPINION,REVIEWPERSON,REVIEWTIME,REVIEW,TYPE) values ('"+gcbh+"','"+opinion+"','"+reviewPerson+"',to_date('"+reviewTime+"','yyyy/mm/dd'),'"+review+"','"+type+"')";

        //获取key值
        string key = DataInfo.Key_encryption("GC_REVIEW_INFO");

        //获取数据
        int data = DataInfo.UpdateTableBySQL(sqltxt,key);
        return data;
    }

     //// 上传施工单位
    [OperationContract]
    public int uploadConstruction(string scgxbh,string scsgdw)
    {
        //sql语句
         string sqltxt  ="update GC_GCXXB t set t.COSTRUCTIONUNIT = '"+scsgdw+"'where t.PROJECTID = '"+scgxbh+"'";
        //获取key值
        string key = DataInfo.Key_encryption("GC_GCXXB");
        //获取数据

        int data = DataInfo.UpdateTableBySQL(sqltxt, key);
        return data;
    }


    //// 修改基本信息
    [OperationContract]
    public int modifyInformation(string xggxbh,string xggcmc,string xggcje,string xggclb,string xggcdz,string xgCOMPANYNAME,string xgAREANAME)
    {
        //sql语句
         string sqltxt  ="update GC_ENGINEERING_INFO t set t.ENAME = '"+xggcmc+"',t.BUDGET='"+xggcje+"',t.CATEGORY='"+xggclb+"',t.ADDRESS='"+xggcdz+"',t.COMPANYID =(select c.COMPANYID from SYS_COMPANIES2 c where c.COMPANYNAME = '"+xgCOMPANYNAME+"'),t.AREAID =(select c.AREAID from SYS_AREAS c where c.AREANAME = '"+xgAREANAME+"') where t.EID = '"+xggxbh+"'";
        //获取key值
        string key = DataInfo.Key_encryption("GC_ENGINEERING_INFO");
        //获取数据

        int data = DataInfo.UpdateTableBySQL(sqltxt, key);
        return data;
    }

    //// 修改工程金额
    [OperationContract]
    public int modifyAmount(string xggxbh,string xggcje)
    {
        //sql语句
         string sqltxt  ="update GC_DESIGN_SCHEME t set t.SCHEMEBUDGET='"+xggcje+"' where t.EID = '"+xggxbh+"'";
        //获取key值
        string key = DataInfo.Key_encryption("GC_DESIGN_SCHEME");
        //获取数据

        int data = DataInfo.UpdateTableBySQL(sqltxt, key);
        return data;
    }

    //// 新建工程
    [OperationContract]
    public int projectInsert(string gcbh,string gcmc,string creatime,string gcdz,string shenji,string sheji,string jldw,string gclb)
    {
        //sql语句
        string sqltxt = "insert into GC_GCXXB(PROJECTID,PROJECTNAME,CREATETIME,ADDRESS,AUDITUNIT,DESIGNUNITS,SUPERVISORYUNIT,CATEGORY) values ('"+gcbh+"','"+gcmc+"',to_date('"+creatime+"','yyyy/mm/dd'),'"+gcdz+"','"+shenji+"','"+sheji+"','"+jldw+"','"+gclb+"')";

        //获取key值
        string key = DataInfo.Key_encryption("GC_GCXXB");

        //获取数据
        int data = DataInfo.UpdateTableBySQL(sqltxt,key);
        return data;
    }

    
    //// 上传资料
    [OperationContract]
    public int uploadPlan(string scgcbh,string scgcmc,string scgclb,string uploader,string upfile,string note)
    {
 
        //sql语句
        string sqltxt = "insert into GC_DATA_INFO (ID,UPLOADTIME,PROJECTID,DATATYPE,UPLOADPERSON,DATAURL,UPLOADREMARK) values((select nvl(max(ID),0)+1 as ID from GC_DATA_INFO),(select sysdate from dual),'"+scgcbh+"','"+scgclb+"','"+uploader+"','"+upfile+"','"+note+"')";


        //获取key值
        string key = DataInfo.Key_encryption("GC_DATA_INFO");

        //获取数据
        int data = DataInfo.UpdateTableBySQL(sqltxt,key);
        return data;
    }


    
    //// 查看附件
    [OperationContract]
    public string attachment(string gcbh)
    {
        //sql语句
        string sqltxt ="select a.*,b.name from GC_DATA_INFO a left join SYS_USERINFO b on a.UPLOADPERSON = b.ID where PROJECTID = '"+gcbh+"' order by UPLOADTIME desc";

        //获取key值
        string key = DataInfo.Key_encryption("GC_DATA_INFO");

        //获取数据
        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }

    
    //// 当前登录者信息
    [OperationContract]
    public string LoginCurrent(string userName)
    {
        //sql语句
        string sqltxt  ="select * from SYS_USERINFO where USERNAME = '"+userName+"'";

        //获取key值
        string key = DataInfo.Key_encryption("SYS_USERINFO");

        //获取数据
        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }
    
   


    ///---------------------------------------------

   
 
//// 查询日志管理数据

    [OperationContract]
    public string Gcrz_info()
    {
        //sql语句
         string sqltxt = "select a.*,b.ename from GC_DAILY_DIARY a left join  GC_ENGINEERING_INFO  b on a.EID=b.eid";

        //获取key值
        string key = DataInfo.Key_encryption("GC_DAILY_DIARY");
        //获取数据

        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }

    //// 查询施工日志数据
    [OperationContract]
    public string Sgrz_info()
    {
        //sql语句
         string sqltxt = "select * from GC_GCRZB where GCBH='OPQLS'or GCBH='DEFGH'";

        //获取key值
        string key = DataInfo.Key_encryption("GC_GCRZB");
        //获取数据

        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }

    /// 查询监理日志数据
    [OperationContract]
    public string checkLog_info(string searchInput,string gczt,string gcTime1,string gcTime2,string gcgs,string gcpq)
    {
         
        // 关键字
        string gcglgjz = "";
        if (searchInput == "")
        {
            gcglgjz = "";
        }
        else
        {
            gcglgjz = "and (ENAME like '%" + searchInput + "%' or EID like '%" + searchInput + "%')";
        }
        // 状态
        string gcglzt = "";
        if(gczt == "全部")
        {
            gcglzt= "";
        }
        else
        {
            gcglzt = "and TYPE = '"+ gczt +"'";
        }
        // 时间
        string gcglshijian = "";
        if (gcTime1 == "")
        {
            gcglshijian = "";
        }
        else
        {
            gcglshijian ="and( UPLOADTIME>=to_date('"+gcTime1 +"','yyyy/mm/dd') and UPLOADTIME<=to_date('"+gcTime2 +"','yyyy/mm/dd') ) ";
        }

        //sql语句
         string sqltxt = "select * from (select a.*,b.ename,b.areaid,b.areaname,b.companyid,b.companyname,c.name from GC_DAILY_DIARY a left join  (select a.*,b.AREANAME,c.COMPANYNAME from GC_ENGINEERING_INFO a left join SYS_AREAS b on a.AREAID = b.AREAID left join SYS_COMPANIES2 c on a.COMPANYID = c.COMPANYID) b on a.EID=b.EID left join SYS_USERINFO c on a.UPLOADPERSON = c.ID) WHERE 1=1 and COMPANYID in("+gcgs+") and AREAID in("+gcpq+") "+gcglgjz+""+gcglzt+""+gcglshijian+"";

        //获取key值
        string key = DataInfo.Key_encryption("GC_DAILY_DIARY");
        //获取数据

        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }
  
    
    



       //// 查询资料管理数据
    [OperationContract]
    public string GCzl_info()
    {
        //sql语句
         string sqltxt = "select a.*,b.ename,c.name as uploadpersonname from GC_DATA_INFO a left join  GC_ENGINEERING_INFO  b on a.PROJECTID=b.eid left join SYS_USERINFO c on a.UPLOADPERSON = c.ID";

        //获取key值
        string key = DataInfo.Key_encryption("GC_DATA_INFO");
        //获取数据

        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }

    // 查询搜索框日志数据
    [OperationContract]
    public string GCrz_selectInfo(string GC_name)
    {
        //sql语句
         string sqltxt = "select * from GC_DATA_INFO where GCBH='"+GC_name+"'";

        //获取key值
        string key = DataInfo.Key_encryption("GC_DATA_INFO");
        //获取数据

        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }
    //// 查询日志关键字数据
    [OperationContract]
    public string Gczl_keyword_info(string searchInput,string zllx,string gcTime1,string gcTime2,string gcgs,string gcpq)
    {
         
        // 关键字
        string gcglgjz = "";
        if (searchInput == "")
        {
            gcglgjz = "";
        }
        else
        {
            gcglgjz = "and (ENAME like '%" + searchInput + "%' or PROJECTID  like '%" + searchInput + "%')";
        }
        // 状态
        string gcglzt = "";
        if(zllx == "全部")
        {
            gcglzt= "";
        }
        else
        {
            gcglzt = "and DATATYPE = '"+ zllx +"'";
        }
        // 时间
        string gcglshijian = "";
        if (gcTime1 == "")
        {
            gcglshijian = "";
        }
        else
        {
            gcglshijian ="and( UPLOADTIME>=to_date('"+gcTime1 +"','yyyy/mm/dd') and UPLOADTIME<=to_date('"+gcTime2 +"','yyyy/mm/dd') ) ";
        }

        //sql语句
         string sqltxt = "select * from (select a.*,b.ename,b.areaid,b.areaname,b.companyid,b.companyname,c.name from GC_DATA_INFO a left join  (select a.*,b.AREANAME,c.COMPANYNAME from GC_ENGINEERING_INFO a left join SYS_AREAS b on a.AREAID = b.AREAID left join SYS_COMPANIES2 c on a.COMPANYID = c.COMPANYID) b on a.PROJECTID=b.eid left join SYS_USERINFO c on a.uploadperson = c.ID) WHERE 1=1 and COMPANYID in("+gcgs+") and AREAID in("+gcpq+") "+gcglgjz+""+gcglzt+""+gcglshijian+"";              
        //获取key值
        string key = DataInfo.Key_encryption("GC_DATA_INFO");
        //获取数据

        string data = DataInfo.QueryTableBySQL(sqltxt, key);
        return data;
    }



    //////---------------

   // 事件考核
[OperationContract]
public string sjkh(string gjz,string gcTime1, string gcTime2,string gcgs,string gcpq)
{
   string gcglgjz = "";
        if (gjz == "")
        {
            gcglgjz = "";
        }
        else
        {
            gcglgjz = "and (ENAME like '%" + gjz + "%' or PROJECTID  like '%" + gjz + "%')";
        }

        // 时间
        string gcglTime= "";
        if (gcTime1 == "")
        {
            gcglTime= "";
        }
        else
        {
            gcglTime ="and( ASSESSTIME>=to_date('"+gcTime1 +" 00:00:00','yyyy/mm/dd  hh24:mi:ss') and ASSESSTIME<=to_date('"+gcTime2 +" 23:59:59','yyyy/mm/dd  hh24:mi:ss') ) ";
        }

     //sql语句
     string sqltext = "select a.*,b.ename,b.areaid,b.areaname,b.companyid,b.companyname from GC_ASSESS a inner join (select a.*,b.AREANAME,c.COMPANYNAME from GC_ENGINEERING_INFO a left join SYS_AREAS b on a.AREAID = b.AREAID left join SYS_COMPANIES2 c on a.COMPANYID = c.COMPANYID) b on a.PROJECTID=b.EID WHERE 1=1 and COMPANYID in ("+gcgs+") and AREAID in("+gcpq+") "+gcglgjz+""+gcglTime+"";    
   //获取key值
   string key = DataInfo.Key_encryption("GC_ASSESS");
   //获取数据
   string data = DataInfo.QueryTableBySQL(sqltext, key);
   return data;
  }


[OperationContract]
public string mapsearch(string name)
{
   //sql语句
   string sqltext = "select COMPANYID,COMPANYNAME from (select distinct parentCOMPANYid from sys_parent_COMPANY_target t where COMPANYid in (select COMPANYid from sys_COMPANY_tree t where parentCOMPANYid = '"+name+"'))t1 left join sys_parent_COMPANY t2 on t1.parentCOMPANYid = t2.COMPANYid";
   //获取key值
   string key = DataInfo.Key_encryption("sys_COMPANY_tree");
   //获取数据
   string data = DataInfo.QueryTableBySQL(sqltext, key);
   return data;
}

    /// <summary>
    /// 签到信息
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="starTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    [OperationContract]
    public string Sign_in_info(string keyword, string starTime, string endTime, string UserID)
    {
        try
        {
            string sqlpqsx1 = "";
            if (keyword != "")
            {
                sqlpqsx1 += " and g.ename like '%"+keyword+"%' ";
            }
            //sqlpqsx1 += " and (t1.SIGNAREA in (select AREAID from sys_AREA_tree where PARENTAREAID  = (select AREAID from SYS_USERINFO where ID = '" + UserID + "')) or t1.SIGNAREA=7 ) ";
            string sqlText = @" select t1.eid,to_char(t1.signintime,'yyyy-mm-dd') as signintime,t2.NAME as SIGNERNAME,t1.SIGNAREA,t1.SIGNCOMPANY,
a.areaname,g.DELFLAG,'0' TYPECODE,
 g.ename as TASKNAME,g.remark as TASKDES,t1.TASKID,t1.SIGNER,COUNT(*) SIGNINNUM,
 sum(ceil((t1.SIGNOUTTIME-t1.SIGNINTIME)*24*60)) DURATION,COUNT(t5.eid) NOSIGN   
 from ( 
 select * from YH_TASK_SIGN where SIGNINTIME>=to_date('{0} 00:00:00','yyyy-mm-dd hh24:mi:ss') 
 and SIGNINTIME<=to_date('{1} 23:59:59','yyyy-mm-dd hh24:mi:ss') ) t1 
 inner join  GC_ENGINEERING_INFO g  on t1.eid=g.eid
 left join 
 ( select * from YH_TASK_SIGN where SIGNINTIME>=to_date('{2} 00:00:00','yyyy-mm-dd hh24:mi:ss') 
 and SIGNINTIME<=to_date('{3} 23:59:59','yyyy-mm-dd hh24:mi:ss') and SIGNOUTTIME is null ) t5  on t1.TASKID=t5.TASKID 
 and  (t1.SIGNINTIME=t5.SIGNINTIME ) 
 left join SYS_USERINFO t2 on t1.SIGNER = t2.ID 
 left join SYS_AREAS a on t1.SIGNAREA=a.AREAID 
where g.DELFLAG='0' {4} 
 group by to_char(t1.signintime,'yyyy-mm-dd') ,t2.NAME ,t1.SIGNAREA,t1.SIGNCOMPANY,
a.areaname,g.DELFLAG,g.ename,g.remark,t1.TASKID,t1.SIGNER,t1.eid ";
            sqlText = string.Format(sqlText, starTime, endTime, starTime, endTime, keyword);

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
            day_num_ID.ColumnName = "EID";
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
                var query1 = table_new.AsEnumerable().Where<DataRow>(a => a["EID"].ToString() == dataTable.Rows[j]["EID"].ToString() && a["SIGNER"].ToString() == dataTable.Rows[j]["SIGNER"].ToString());
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
                    dr["EID"] = dataTable.Rows[j]["EID"].ToString();
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
///签到信息查询
[OperationContract]
public string SignInInfoSelect(string pro,string name,string startTime,string endTime){
   //sql语句
   string sqltext = "select a.*,b.ENAME,c.NAME,d.COMPANYNAME,e.AREANAME from YH_TASK_SIGN a left join GC_ENGINEERING_INFO b on a.EID = b.EID left join SYS_USERINFO c on a.SIGNER = c.ID left join SYS_COMPANIES2 d on a.SIGNCOMPANY = d.COMPANYID left join SYS_AREAS e on a.SIGNAREA = e.AREAID where a.EID is not null and a.signer='"+name+"' and a.Eid='"+pro+"' and (a.Signintime between to_date('"+startTime+" 00:00:00','yyyy/MM/dd hh24:mi:ss') and to_date('"+endTime+" 23:59:59','yyyy/MM/dd hh24:mi:ss'))";
   //获取key值
   string key = DataInfo.Key_encryption("YH_TASK_SIGN");
   //获取数据
   string data = DataInfo.QueryTableBySQL(sqltext, key);
   return data;
}
}
 