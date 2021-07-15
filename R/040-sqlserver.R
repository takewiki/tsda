#' 定义排序判断转换函数
#'
#' @param x 逻辑变量
#'
#' @return 返回值
#'
#' @examples logical_as_asc();
logical_as_asc <- function(x){
 if (x){
   'asc'
 }else{
   'desc'
 }
}

#' 排序判断辅助函数
#'
#' @param logical_vec 逻辑变量
#'
#' @return 返回值
#'
#' @examples logical_as_ascs();
logical_as_ascs <-function(logical_vec){
  unlist(lapply(logical_vec, logical_as_asc))
}
#' 定义sqlserver的语句自动生成函数
#'
#' @param table_name 表名称
#' @param col_vec 字段名向量
#' @param row_where_expr where表达式，完全使用tsql语法
#' @param order_vec   排序字段
#' @param order_asc_logical 逻辑表达式TRUE表示asc,FALSE表示desc
#'
#' @return 返回值
#' @include 040-aaaparam.R
#' @export
#'
#' @examples
#' bb <- select_gen_sqlserver(table_name='T_GL_VOUCHER',
#' col_vec=c('FDATE','FYEAR','FPERIOD','FBILLNO'),
#' row_where_expr="FDATE>='2019-01-01' and fdate <= '2019-03-31' ",order_vec=c('FDATE','FBILLNO'),order_asc_logical=c(T,T));
#' bb;
#' cc <- select_gen_sqlserver(table_name='T_GL_VOUCHER',
#' col_vec=c('FDATE','FYEAR','FPERIOD','FBILLNO'),
#' row_where_expr=NULL,order_vec=c('FDATE','FBILLNO'),order_asc_logical=c(F,T));
#' cc;
#' dd <- select_gen_sqlserver(table_name='T_GL_VOUCHER',
#' col_vec=c('FDATE','FYEAR','FPERIOD','FBILLNO'),
#' row_where_expr=NULL);
#' dd;
select_gen_sqlserver <- function(table_name='T_GL_VOUCHER',
                                 col_vec=c('FDATE','FYEAR','FPERIOD','FBILLNO'),
                                 row_where_expr=NULL,order_vec=NULL,order_asc_logical=NULL) {

#定义拼接的字段
  prefix <- 'select ';
  field_str <- paste(col_vec,collapse = ',');
  from_str <-'   from  ';
  where_str <-'  where   ';
  order_by <-'  order by  '
  if( !is.null(order_vec)){
    # 针对排序定义了辅助函数
    order_str<- paste(order_vec,logical_as_ascs(order_asc_logical),sep = " ",collapse = ',')
    if(!is.null(row_where_expr)){
      #最全的情况
      paste(prefix,field_str,from_str,table_name,where_str,row_where_expr,order_by,order_str,sep="");
    }else{
      #没有where，需要排序
      paste(prefix,field_str,from_str,table_name,order_by,order_str,sep="");
    }
  }else{
    #以下不需要排序,带where
    if(!is.null(row_where_expr)){
      paste(prefix,field_str,from_str,table_name,where_str,row_where_expr,sep="");
    }else{
      #全表查询
      paste(prefix,field_str,from_str,table_name,"   ",sep="");
    }
  }
}







#' 将字段列表进行处理
#'
#' @param fieldName_vec 字段列表
#' @param fieldName_caption  字段标题
#'
#' @return 返回值
#' @export
#'
#' @examples simpleFieldCombiner();
#' simpleFieldCombiner(letters,LETTERS);
#' simpleFieldCombiner(c('fnumber','fname'),c('物料代码','物料名称'));
simpleFieldCombiner_sqlserver <-function(fieldName_vec,fieldName_caption=NULL){
  if(is.null(fieldName_caption)){
    fieldName_caption <- fieldName_vec;
  }
  res <-paste(" ",fieldName_vec,'as',fieldName_caption," ",sep = " ",collapse = ",");
  return(res)
}

#' 将聚合字段列表
#'
#' @param fieldName_vec 字段列表
#' @param fieldName_caption 字段标题
#' @param agg_fun_vec 聚合函数
#'
#' @return 返回值
#' @export
#'
#' @examples aggregateFieldCombiner();
#' aggregateFieldCombiner(c('aa','bb'),c('物料数量','物料金额'),c('sum','sum'));
#' aggregateFieldCombiner(c('aa','bb'),aggr_fun_vec = c('sum','sum'));
aggregateFieldCombiner_sqlserver <- function(fieldName_vec=NULL,fieldName_caption=NULL,aggr_fun_vec=NULL)
{
  if(is.null(fieldName_vec)){
    res <-NULL
  }else{
    if(is.null(fieldName_caption)){
      fieldName_caption <- fieldName_vec;
    }
    res <-paste(" ",aggr_fun_vec,"(",fieldName_vec,")",' as ',fieldName_caption," ",sep = "",collapse = ",");
    res <-paste(" , ",res,sep="");

  }

  return(res)
}

#' 确定分组字段
#'
#' @param fieldName_vec 分组字段列表
#'
#' @return 返回值
#' @export
#'
#' @examples groupByCombiner_sqlserver();
#' groupByCombiner_sqlserver(LETTERS);
groupByCombiner_sqlserver <- function(fieldName_vec=NULL){
  if (is.null(fieldName_vec)){
    res <-NULL
  }else{
    res <-paste(" ",fieldName_vec," ",sep="",collapse = ",");
  }

  return(res);
}



#' 形成一个where子句
#'
#' @param fieldName_vec 字段列表
#' @param comparerSig_vec  比例符>=<
#' @param filterValue_vec 比例值
#' @param comboCondition_logi_vec  and或者or最后一个逻辑不生效
#'
#' @return 返回值
#' @export
#'
#' @examples  whereStatementCombiner();
#' is.null(whereStatementCombiner());
#' fieldName_vec <-c('FYEAR','FDATE');
#' comparerSig_vec <-c('=','>=');
#' filterValue_vec <-c("2018","'2018-10-02'");
#' comboCondition_logi_vec <-c('and','and');
#' whereStatementCombiner(fieldName_vec,comparerSig_vec,filterValue_vec,comboCondition_logi_vec);
whereStatementCombiner_sqlserver <- function(fieldName_vec=NULL,comparerSig_vec=NULL,filterValue_vec=NULL,comboCondition_logi_vec=NULL){
  if(is.null(fieldName_vec)){
    res <-NULL
  }else{
    logi_len <- length(comboCondition_logi_vec);
    comboCondition_logi_vec[logi_len] <- " ";

    res <-paste(" ",fieldName_vec,comparerSig_vec,filterValue_vec,comboCondition_logi_vec,sep=" ",collapse = "");

  }


  return(res);


}

#' 处理order By子句
#'
#' @param fieldName_vec 字段列表
#' @param asc_vec 排序向量
#'
#' @return 返回值
#' @export
#'
#' @examples orderByCombiner();
#' orderByCombiner();
#' orderByCombiner(fieldName_vec = letters,asc_vec = rep(T,26));
orderByCombiner_sqlserver <- function(fieldName_vec=NULL,asc_vec=NULL) {
  if (is.null(fieldName_vec)){
    res <-NULL
  }else{
    asc_str <- logical_as_ascs(asc_vec);
   res <- paste(" ",fieldName_vec," ",asc_str," ",sep = "",collapse = ",")
  }
  return(res);

}



#' 用于sqlsql select 生成器
#'
#' @param fieldName_vec_simple 普通字段列表
#' @param table_name  表名称
#' @param fieldName_caption_simple 普通字段标题
#' @param fieldName_vec_aggr 聚合字段列表
#' @param fieldName_caption_aggr 字段字段列表
#' @param fun_vec_aggr 聚合函数
#' @param fieldName_vec_where  行过滤字段列表
#' @param comparerSig_vec_where 行比较符
#' @param filterValue_vec_where 行比值值
#' @param comboCondition_logi_vec_where 多行连接
#' @param fieldName_vec_groupBy 分组字段
#' @param fieldName_vec_orderBy 排序字段列表
#' @param asc_vec_orderBy  字段升降排序
#'
#' @return 返回值
#' @export
#'
#' @examples select_engine_sqlserver();
#' #test 01 只显示字段列表--------
#' # select  FVOUCHERGROUPNO,FYEAR,FPERIOD,FBILLNO from T_GL_VOUCHER
#' fieldName_vec_simple <-c('FVOUCHERGROUPNO','FYEAR','FPERIOD','FBILLNO');
#' table_name ='T_GL_VOUCHER';
#'simpleFieldCombiner_sqlserver(fieldName_vec_simple);
#'
#' test01 <- select_engine_sqlserver(fieldName_vec_simple =fieldName_vec_simple ,
#'                                  table_name = table_name
#' );
#' test01;
#' #test 02 显示字段列表及别名--------
#' # select    FVOUCHERGROUPNO as 凭证字号  ,  FYEAR as 会计年度  ,  FPERIOD as 会计期间  ,
#' # FBILLNO as 凭证号      from  T_GL_VOUCHER
#'
#'fieldName_vec_simple <-c('FVOUCHERGROUPNO','FYEAR','FPERIOD','FBILLNO');
#' table_name ='T_GL_VOUCHER';
#' fieldName_caption_simple <-c('凭证字号','会计年度','会计期间','凭证号');
#'
#'test02 <- select_engine_sqlserver(fieldName_vec_simple =fieldName_vec_simple ,
#'                                  fieldName_caption_simple = fieldName_caption_simple,
#'                                  table_name = table_name
#');
#'test02;
#'#test 03 显示字段列表及别名,显示行过滤--------
#'# select    FVOUCHERGROUPNO as 凭证字号  ,  FYEAR as 会计年度  ,
#'# FPERIOD as 会计期间  ,  FBILLNO as 凭证号      from  T_GL_VOUCHER
#'# where   FDATE = '2018-10-09' and
#'# FVOUCHERGROUPID = 1 and  FACCOUNTBOOKID = 100032
#'fieldName_vec_simple <-c('FVOUCHERGROUPNO','FYEAR','FPERIOD','FBILLNO');
#'table_name ='T_GL_VOUCHER';
#'fieldName_caption_simple <-c('凭证字号','会计年度','会计期间','凭证号');
#'fieldName_vec_where = c('FDATE','FVOUCHERGROUPID','FACCOUNTBOOKID');
#'comparerSig_vec_where = c('=','=','=');
#'filterValue_vec_where = c("'2018-10-09'","1","100032");
#'comboCondition_logi_vec_where = c('and','and','and');
#'test03 <- select_engine_sqlserver(fieldName_vec_simple =fieldName_vec_simple ,
#'                                  fieldName_vec_where = fieldName_vec_where,
#'                                  comparerSig_vec_where = comparerSig_vec_where,
#'                                  filterValue_vec_where = filterValue_vec_where,
#'                                  comboCondition_logi_vec_where =comboCondition_logi_vec_where ,
#'                                  fieldName_caption_simple = fieldName_caption_simple,
#'                                  table_name = table_name
#');
#'test03;
#'#test 04 显示字段列表及别名,显示行过滤,字段排序--------
#'# select    FVOUCHERGROUPNO as 凭证字号  ,
#'# FYEAR as 会计年度  ,  FPERIOD as 会计期间  ,
#'# FBILLNO as 凭证号      from  T_GL_VOUCHER
#'# where   FDATE = '2018-10-09' and  FVOUCHERGROUPID = 1
#'# and  FACCOUNTBOOKID = 100032
#'# order by  FVOUCHERGROUPNO asc , FBILLNO desc
#'fieldName_vec_simple <-c('FVOUCHERGROUPNO','FYEAR','FPERIOD','FBILLNO');
#'table_name ='T_GL_VOUCHER';
#'fieldName_caption_simple <-c('凭证字号','会计年度','会计期间','凭证号');
#'fieldName_vec_where = c('FDATE','FVOUCHERGROUPID','FACCOUNTBOOKID');
#'comparerSig_vec_where = c('=','=','=');
#'filterValue_vec_where = c("'2018-10-09'","1","100032");
#'comboCondition_logi_vec_where = c('and','and','and');
#'fieldName_vec_orderBy = c('FVOUCHERGROUPNO','FBILLNO')
#'asc_vec_orderBy = c(T,F)
#'test04 <- select_engine_sqlserver(fieldName_vec_simple =fieldName_vec_simple ,
#'                                  fieldName_vec_orderBy = fieldName_vec_orderBy,
#'                                  asc_vec_orderBy = asc_vec_orderBy,
#'                                  fieldName_vec_where = fieldName_vec_where,
#'                                  comparerSig_vec_where = comparerSig_vec_where,
#'                                  filterValue_vec_where = filterValue_vec_where,
#'                                  comboCondition_logi_vec_where =comboCondition_logi_vec_where ,
#'                                  fieldName_caption_simple = fieldName_caption_simple,
#'                                  table_name = table_name
#');
#'test04;
#'test05 <-select_engine_sqlserver(fieldName_vec_simple = c('FVOUCHERGROUPID'),
#'fieldName_vec_where = 'FVOUCHERGROUPID',
#'comparerSig_vec_where = '<>',
#'filterValue_vec_where = '1',
#'comboCondition_logi_vec_where = 'and',
#'fieldName_vec_groupBy = 'FVOUCHERGROUPID',
#'fieldName_vec_orderBy = 'FVOUCHERGROUPID',
#'asc_vec_orderBy = F,
#'fun_vec_aggr = c('count','sum'),
#'fieldName_vec_aggr = c("FBILLNO","FPRINTTIMES"),
#'fieldName_caption_aggr =c("bill_count","sum_print"),
#'table_name = 'T_GL_VOUCHER'
#'
#')
#'test05;
select_engine_sqlserver <-function(fieldName_vec_simple,
                                   table_name,
                                   fieldName_caption_simple=NULL,
                                   fieldName_vec_aggr=NULL,
                                   fieldName_caption_aggr=NULL,
                                   fun_vec_aggr=NULL,
                                   fieldName_vec_where=NULL,
                                   comparerSig_vec_where=NULL,
                                   filterValue_vec_where=NULL,
                                   comboCondition_logi_vec_where=NULL,
                                   fieldName_vec_groupBy=NULL,
                                   fieldName_vec_orderBy=NULL,
                                   asc_vec_orderBy=NULL
){
  simple_field_str <-simpleFieldCombiner_sqlserver(fieldName_vec = fieldName_vec_simple,
                                                   fieldName_caption = fieldName_caption_simple
                                                    );
  aggr_field_str <- aggregateFieldCombiner_sqlserver(fieldName_vec = fieldName_vec_aggr,
                                                     fieldName_caption = fieldName_caption_aggr,
                                                     aggr_fun_vec = fun_vec_aggr);
  where_str <- whereStatementCombiner_sqlserver(fieldName_vec = fieldName_vec_where,
                                                comparerSig_vec = comparerSig_vec_where,
                                                filterValue_vec = filterValue_vec_where,
                                                comboCondition_logi_vec = comboCondition_logi_vec_where);
  groupBy_str <- groupByCombiner_sqlserver(fieldName_vec = fieldName_vec_groupBy);
  orderBy_str <-orderByCombiner_sqlserver(fieldName_vec = fieldName_vec_orderBy,
                                          asc_vec = asc_vec_orderBy);

 if(is.null(aggr_field_str)){
   aggr_field_str <-" ";
 }
 if(is.null(where_str)){
   where_str <- " "
 }else{
   where_str <-paste(' where ',where_str," ",sep="");
 }
 if(is.null(groupBy_str)){
   groupBy_str <-" "
 }else{
   groupBy_str <- paste(" group by ",groupBy_str," ",sep="");
 }
 if(is.null(orderBy_str)){
   orderBy_str <-" "
 }else{
   orderBy_str<-paste(" order by ",orderBy_str," ",sep="");
 }
  res <- paste('select ',simple_field_str,aggr_field_str," from ",table_name," ",where_str,groupBy_str,orderBy_str,sep=" ");
  return(res)
}


#' 读取棱星数据库的配置信息
#'
#' @param db_name 数据库名称,其他信息已加密处理
#'
#' @return 返回值
#' @export
#'
#' @examples sql_conn_rd
sql_conn_rd <- function(db_name='AIS20190427230019') {
  conn_demo_setting(db_name)
}


#' 设置通用的数据库连接
#'
#' @param ip 服务器地址
#' @param port 服务器端口
#' @param user_name  用户名
#' @param password  密码
#' @param db_name  数据库名称
#'
#' @return 返回值
#' @import RJDBC
#' @export
#'
#' @examples
#' sql_conn_common()
sql_conn_common <- function(ip='115.159.201.178',
                            port=1433,
                            user_name='sa',
                            password='Hoolilay889@',
                            db_name='JH_2018B'
                            ){

  drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","/opt/jdbc/mssql-jdbc-7.2.2.jre8.jar")
  con_str <- paste("jdbc:sqlserver://",ip,":",port,";databaseName=",db_name,sep="");
  con <- dbConnect(drv, con_str, user_name, password)
  return(con)

}


#' 添加sql 连接函数
#'
#' @param ip 服务器地址
#' @param port 端口
#' @param user_name 用户名
#' @param password  密码
#' @param db_name 数据库
#'
#' @return 返回链接
#' @export
#'
#' @examples
#' sql_conn
sql_conn <- function(ip,
                     port=1433,
                     user_name='sa',
                     password,
                     db_name='test'){
  res <- sql_conn_common(ip,
                         port,
                         user_name,
                         password,
                         db_name)
  return(res)
}

#' 测试连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' test_conn()
test_conn <-function()
{
  conn <- sql_conn_common(db_name = 'test')
  sql <- 'select * from conn'
  res <-sql_select(conn,sql)
  if(nrow(res) ==1){
    res <-res$fname
  }else{
    res <-'连接失败'
  }
  return(res)

}

#' 测试连接
#'
#' @return 返回值
#' @export
#'
#' @examples
#' test_conn()
test_conn2 <-function()
{
  conn <- sql_conn(ip = '115.159.201.178',port = 1433,user_name = 'sa',password = 'Hoolilay889@',db_name = 'test')
  sql <- 'select * from conn'
  res <-sql_select(conn,sql)
  if(nrow(res) ==1){
    res <-res$fname
  }else{
    res <-'连接失败'
  }
  return(res)

}



#' 获取rds数据库服务
#'
#' @param db_name 数据库名称
#'
#' @return 返回连接
#' @export
#'
#' @examples
#' conn_rds
conn_rds <- function(db_name='test') {
  conn <-sql_conn_common(db_name = db_name)
  return(conn)

}





#' 查询指定表的字段类型
#'
#' @param conn 连接信息
#' @param table_name 表名
#'
#' @return 返回数据库
#' @export
#'
#' @examples
#' sql_fieldInfo()
sql_fieldInfo <- function(conn=conn_rds(),table_name='books')
{
  sql <- paste0("select a.name as FFieldName,b.name as FTypeName from sys.columns  a
inner join sys.types b
on a.system_type_id = b.user_type_id

where a.object_id=object_id('",table_name,"') ")
  #print(sql)
  res <- sql_select(conn,sql)
  return(res)
}

#' 针对列表查询
#'
#' @param table 表名
#' @param fieldNames 字段清单，向量定义，不建议使用*
#' @param conn 连接信息
#'
#' @return 返回值
#' @export
#'
#' @examples
#' sql_gen_select(table = 't_test')
#' sql_gen_select(table = 'conn')
#' sql_gen_select()
sql_gen_select <- function(conn=conn_rds('test'),table='books',fieldNames=NULL) {


  if(is.null(fieldNames)){
    fieldList <-sql_fieldInfo(conn,table)
    fieldNames <- fieldList$FFieldName
  }
    field_str <- paste(' ',fieldNames,' ',sep = '',collapse = ',')
  res <- paste0('select ',field_str, ' from ',table)

  return(res)


}



#' 生成insert相关语句
#'
#' @param conn 连接
#' @param table 表名
#' @param fieldNames 字段名
#'
#' @return 返回值
#' @export
#'
#' @examples
#' sql_gen_insert()
sql_gen_insert <- function(conn=conn_rds('test'),table='books',fieldNames=NULL) {


  if(is.null(fieldNames)){
    fieldList <-sql_fieldInfo(conn,table)
    fieldNames <- fieldList$FFieldName
  }
  field_str <- paste(' ',fieldNames,' ',sep = '',collapse = ',')
  res <- paste0('insert into  ',table,' (',field_str, ') values ( ')

  return(res)


}


#' 设置更新的标题
#'
#' @param table 表名
#'
#' @return 返回值
#' @export
#'
#' @examples
#' sql_gen_update()
sql_gen_update <- function(table='books') {



  res <- paste0('update  ',table,'  set  ')

  return(res)


}


#' 创建sql更新语句
#'
#' @param table  表名
#'
#' @return 返回值
#' @export
#'
#' @examples
#' sql_gen_delete()
sql_gen_delete <- function(table='books') {



  res <- paste0('delete  from   ',table,'  where  ')

  return(res)


}




#' 上传数据到数据，增量部分
#'
#' @param conn 数据库连接
#' @param table_name 表名
#' @param data 追加数据
#'
#' @return 返回值
#' @export
#'
#' @examples
#' upload_data()
upload_data <- function(conn,table_name,data) {

  sql <- paste0("select * from ",table_name);
  oldData <- sql_select(conn,sql)
  #原有数据的记录行数
  ncount_old <- nrow(oldData)
  ncount_new <-nrow(data)

  if (ncount_old == 0 & ncount_new > 0){
    res <- data
    tsda::db_writeTable(conn=conn,table_name = table_name,r_object = res,append = T)
    status <-TRUE
  }else if (ncount_old ==0 & ncount_new == 0 )
  {
    #warning("请检查数据")
    status <- FALSE
  }else if(ncount_old >0 & ncount_new >0){
    data_diff <- tsdo::df_setdiff(data,oldData)
    ncount_diff <- nrow(data_diff)
    if(ncount_diff >0){
      res <- data_diff
      tsda::db_writeTable(conn=conn,table_name = table_name,r_object = res,append = T)
      status <-TRUE
    }else{
      #warning("没有新数据")
      status <- FALSE
    }
  }

  return(status)


}

#' 获取连接
#'
#' @return 返回连接
#' @export
#'
#' @examples
#' conn_rds_nsic()
conn_rds_nsic <- function() {
  res <- conn_rds('nsic')
  return(res)

}



#'获取字段列名
#'
#' @param conn 连接
#' @param table_name  表名
#'
#' @return 返回值
#' @export
#'
#' @examples
#' db_getColName()
db_getColName <- function(conn=tsda::conn_rds('jlrds'),table_name='t_mrpt_division') {
  sql <- paste0("select a.name as FName   from sys.columns a
inner join sys.objects b
on a.object_id = b.object_id
 where  b.name ='",table_name,"'")
  r <- sql_select(conn,sql)
  ncount <- nrow(r)
  if(ncount >0){
    res <- r$FName
  }else{
    res <- NULL
  }
  return(res)

}
