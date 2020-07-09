package com.company.hudi

import com.company.avro.ParameterTool
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession;

object ConvertHiveTableToHudiTest {


  def main(args: Array[String]): Unit = {
    // 1.获取参数
    val params = ParameterTool.fromArgs(args);

    // 2.spark环境变量
    val sparkConf = new SparkConf()
      .setAppName("ConvertHiveTableToHudiTest")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

//    -- hudi: 200970453
//    -- full: 200574005
//    select count(*) from ods_test.sellercube_productcustomerlabel
//
//    -- productcustomerlabel 2020-06-02：68392367968
//    -- productcustomerlabel 2020-06-03：68457603576
//    -- full:      279819
//    -- sqlserver: 279364
//    -- hudi:      279329
//    select count(*) from ods_test.sellercube_productcustomerlabel where modifytimestamp >=68392367968 and modifytimestamp <=68457603576

    val hiveSQL = "select *,substr(importdatetime,1,7) as hudi_pt from ods_test.sq8flyt_orderparent"
    val hudiBasePath ="/apps/hive/warehouse/hudi.db"
    val hudiTableName = "sq8flyt_orderparent"
    val hudiTablePath = s"$hudiBasePath/$hudiTableName"
    val database = "hudi"
    val recordkeyField = "id"
    val partitionpathField = "hudi_pt"
    val precombineField = "modifytimestamp"

    // 3.加载hudi表
    spark.read.format("hudi").load(hudiTablePath +"/" +hudiTableName +"/*/*").createOrReplaceTempView(hudiTableName)


    spark.sql(s"SELECT count(*) from $hudiTableName ").show()

    spark.sql(s"SELECT\ncount(*)\nfrom $hudiTableName\nWHERE modifytimestamp >= 68392367968 and modifytimestamp <= 68457603576").show()
    spark.sql(
      s"""SELECT
         |labelid,count(*)
         |from $hudiTableName
         |group by labelid
         |having count(*) > 1
         |""".stripMargin).show(10)

    spark.sql(
      s"""SELECT
         |*
         |from $hudiTableName
         |where labelid in (321750348)
         |limit 10
         |""".stripMargin).show()


    spark.sql(
      s"""SELECT
         |adddatetime,unix_timestamp(adddatetime),labelid
         |from $hudiTableName
         |limit 10
         |""".stripMargin).show()

    s"""
       |insert overwrite table st.st_fba_refund_01 partition(stat_date)
       |
       |select
       |   s.product_id                --产品ID
       |  ,s.product_code              --'SKU'
       |  ,count(distinct case when s.s_order_id='' then s.i_order_id else s.s_order_id end) bl_send_sale_qty    --'已发货总订单数(所属期)'
       |  ,count(distinct r.order_id )        bl_send_refund_qty  --'已发货退款订单数(所属期)'
       |  ,count(distinct case when  r.lable_sub ='服装；尺寸偏大' and r.order_id<>'' then r.order_id else null end)  big_refund_qty--服装；尺码偏大
       |  ,count(distinct case when  r.lable_sub ='服装；尺寸偏小' and r.order_id<>'' then r.order_id else null end)  small_refund_qty--服装；尺码偏小
       |  ,count(distinct case when  r.lable_sub ='不喜欢面料' and r.order_id<>'' then r.order_id else null end)  n_like_refund_qty--不喜欢面料
       |  ,count(distinct case when  r.lable_sub ='不喜欢颜色' and r.order_id<>'' then r.order_id else null end)  n_color_refund_qty--不喜欢颜色
       |  ,count(distinct case when  r.lable_sub ='与网站描述不符' and r.order_id<>'' then r.order_id else null end)  n_situable_refund_qty--与网站描述不符
       |  ,count(distinct case when  r.lable_sub ='质量问题' and r.order_id<>'' then r.order_id else null end)  quality_refund_qty--质量问题
       |  ,count(distinct case when  r.lable_sub ='瑕疵品' and r.order_id<>'' then r.order_id else null end)  defect_refund_qty--瑕疵品
       |  ,count(distinct case when  r.lable_sub not in ('服装；尺寸偏大','服装；尺寸偏小','不喜欢面料','不喜欢颜色','与网站描述不符','质量问题','瑕疵品') and r.order_id<>'' then r.order_id else null end)  other_refund_qty--其他问题
       |  ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') ins_date--'数据插入时间戳'
       |  ,s.stat_date
       |from (
       |select * from tmp.st_all_platform_refund_01 where is_selfsend='否'--平台仓发货订单
       |union
       |select * from tmp.st_all_platform_refund_02 where is_selfsend='是'  --自发货仓发货订单  bysjq20200330加是自发货条件判断，避免重复
       |) s--已发货订单
       |left join dw.st_sku_poa_refund_order_p r on r.order_id=(case when s.s_order_id='' then s.i_order_id else s.s_order_id end)--退款宽表
       |                                        and r.product_id=s.product_id and r.property_id=s.property_id and r.is_delivery='是'
       |where s.is_fba='是'
       |group by
       |   s.stat_date
       |  ,s.product_id                --产品ID
       |  ,s.product_code              --'SKU'
       |;
       |""".stripMargin

    spark.sql("SELECT adddatetime,unix_timestamp(adddatetime),labelid from ods_test.sellercube_productcustomerlabel WHERE labelid =189252997").show()



    // 4.执行测试
    spark.sql(s"select * from $hudiTableName where id = 'A000272004231E0A' ").show()
    spark.sql(s"select count(*) from $hudiTableName").show()
    spark.sql(s"select importdate,count(*) from $hudiTableName group by importdate").show()
    spark.sql(s"select country,sum(OriginalTotalPrice) from $hudiTableName group by country").show(100)

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(
      s"""insert overwrite table tmp.lcc_dw_order partition(part_dt)
         |select
         |	 op.id as order_id
         |	,op.importdatetime as import_time
         |	,e.Category as platform_id
         |	,nvl(pf.PlatformName,'') as platform_name
         |	,nvl(e.ebayid,0) as shop_id
         |	,trim(op.flytDetailShop) as shop_name
         |	,cast(op.totalprice as decimal(18,4)) as pay_amount
         |	,cast(op.totalprice as decimal(18,4))/prh.rateCN as pay_amount_cn
         |	,cast(op.freight as decimal(18,4)) as freight
         |	,case when nvl(trim(op.ebayuserid),'')='' then op.id else nvl(trim(op.ebayuserid),'') end as customer_id
         |	,op.saletime as sale_time
         |	,op.paytime as pay_time
         |	,case when op.shipworksstoreid='AmazonFBA.com' then op.importdatetime else  op.jjdate end as jiaoji_time
         |	,op.senddate as send_time
         |	,op.checkouttime as checkout_time --
         |	,op.changestatetime as change_state_time
         |	,nvl(op.sortgentime,'1900-01-01 00:00:00') as sort_time
         |	,nvl(op.receivetime,'1900-01-01 00:00:00') as receive_time
         |	,op.flytuserid as flyt_user_id
         |	,nvl(op.location,'') as site
         |	,nvl(op.ordertype,'') as order_type
         |	,op.paymenttype as pay_type_id
         |	,nvl(pm.paymenttypename,'') as pay_type_name
         |	,op.state as order_status_id
         |	,st.statename as order_status_name
         |	,op.countryid as country_id
         |	,ct.jtname as country_name
         |	,nvl(op.posttype,0) as post_type_id
         |	,nvl(pt.type,'') as post_type_name
         |	,op.shipping_service as shipping_service
         |	,op.processcenterid as processcenter_id
         |	,pc.name as processcenter_name
         |	,nvl(op.jjuser,'') as jiaoji_user_name
         |	,nvl(op.isRepeatSent,0) as is_repeat_sent
         |	,nvl(op.ProfitReport,0) as is_profit_report
         |	,nvl(op.isDelivery,0) as is_delivery
         |	,nvl(op.IsParent,0) as is_parent
         |	,nvl(op.IsSplit,0) as is_split
         |	,nvl(op.IsReplenishment,0) as is_replenishment
         |	,nvl(op.isPPAudit,0) as is_pp_audit
         |	,nvl(op.IsOriginal,0) as is_original
         |	,nvl(op.setting,'') as setting_code
         |	,case
         |		when op.setting='holdup' then '申请拦截'
         |		when op.setting='repeat' then '申请重发'
         |		when op.setting='return' then '申请退回'
         |		else ''
         |	end as setting_name
         |	,op.DeliveryWarehouse as delivery_warehouse_id
         |	,case op.DeliveryWarehouse
         |		when 0 then '默认'
         |		when 1 then '捷网'
         |		when 2 then '尖货'
         |		when 3 then 'AE平台仓(HBA)'
         |		when 4 then 'Jumia平台仓(FBJ)'
         |		when 5 then 'Lazada平台仓(FBL)'
         |		when 6 then 'CD平台(FBC)'
         |		when 7 then 'WISH平台(FBW)'
         |		when 8 then '亚马逊平台仓(FBA)'
         |		when 9 then 'Yandex平台仓(FBY)'
         |		else ''
         |	end as delivery_warehouse --
         |	,op.ChannelTrackStatus as channel_track_status_id
         |	,case op.ChannelTrackStatus
         |		when 0 then '默认'
         |		when 1 then '可追踪'
         |		when 2 then '需要妥投'
         |		when 3 then 'WE订单'
         |		else ''
         |	end channel_track_status
         |	,nvl(op.OrderSourceType,0) as order_source_type_id
         |	,case op.OrderSourceType
         |		when 0 then '接口传输'
         |		when 1 then 'OA手工录入'
         |		when 2 then 'OA手工导入'
         |		else ''
         |	end as order_source_type
         |	,op.receivename as receive_name
         |	,op.phone
         |	,op.city
         |	,op.county
         |	,op.zip as zip_code
         |	,case op.shipworksstoreid
         |		when 'AmazonFBA.com' then 'FBA'
         |		when 'Amazon.com' then 'FBM'
         |		else ''
         |	end as is_fba
         |	,cast(op.weight2 as decimal(18,4)) as weight2
         |	,cast(op.VolumeWeight as decimal(18,4)) as volume_weight
         |	,op.traceID as trace_id
         |	,op.ProductManagerID AS product_manager_id
         |	,u.username as product_manager_name
         |	,nvl(op.salesuserid,0) as sales_user_id
         |	,u1.username as sales_user_name
         |	,nvl(op.productspecialerid,0) as product_specialer_id
         |	,u2.username as product_specialer_name
         |	,op.shipworksorder as ship_works_order
         |	,nvl(case
         |		when e.Category=12 then op.productspecialerid
         |		when e.Category=1 then p.bg_manager_id
         |		when e.Category=18 then p.ys_manager_id
         |		when e.Category=24 then p.nc_manager_id
         |		when e.Category=40 then p.secrexy_manager_id
         |		when e.Category=65 then p.koyye_manager_id
         |		when e.Category=13 then p.ae_manager_id
         |		when e.Category=63 then p.dev_manager_id
         |		else op.ProductManagerID
         |	end,0) as order_manager_id
         |	,nvl(case
         |		when e.Category=12 then u2.username
         |		when e.Category=1 then p.bg_manager_name
         |		when e.Category=18 then p.ys_manager_name
         |		when e.Category=24 then p.nc_manager_name
         |		when e.Category=40 then p.secrexy_manager_name
         |		when e.Category=65 then p.koyye_manager_name
         |		when e.Category=13 then p.ae_manager_name
         |		when e.Category=63 then p.dev_manager_name
         |		else u.username
         |	 end,'') as order_manager_name
         |       ,nvl(op.paypaltransactionid,'')  as paypal_transaction_id
         |	,current_timestamp() as ins_date
         |	,to_date(op.importdatetime) as part_dt
         |from $hudiTableName op
         |join OpenData.ebay e on upper(trim(e.ebayname))=upper(trim(op.flytdetailshop))
         |	and e.companyid=6
         |	and e.ebayname not in ('1','11')
         |	and op.importdatetime>=date_add(current_date(),-250)
         |	and nvl(op.sysevent,'')<>'DELETE'
         |left join sellercube.platform pf on e.Category=pf.PlatformNo
         |left join sq8flyt.state st on st.id=op.state
         |left join sq8flyt.countrys ct on ct.id=op.countryid
         |left join sq8flyt.posttype pt on pt.id=op.posttype
         |left join sellercube.ProcessCenter pc on pc.processCenterid=op.processcenterid
         |left join sellercube.Paymenttype pm on pm.id=op.PaymentType
         |left join (select *,row_number()over(partition by name,to_date(updatetime) order by updatetime desc) rownum from OpenData.PaypalExchangeRateHistory) prh on prh.rownum=1 and prh.code='USD' and to_date(op.importdatetime)=to_date(prh.updatetime)
         |left join sellercube.users u on u.userid=op.ProductManagerID
         |left join sellercube.users u1 on u1.userid=op.salesuserid
         |left join sellercube.users u2 on u2.userid=op.productspecialerid
         |left join dw.dw_order_product_items od on od.part_dt=to_date(op.importdatetime) and od.order_id=op.id and od.sale_amount_rank=1 and od.platform_id in (1,18,24,40,65,13,63)
         |left join dim.dim_product p on od.product_id=p.product_id
         |distribute by part_dt""".stripMargin)
  }
}
