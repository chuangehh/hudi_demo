package com.company.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringReplace {

  private static void sqlServerSelect() {
    StringBuilder sb = new StringBuilder();

    String targetTable = TEMPLATE_FIELD.get("${targetTable}");
    List<String> targetTableDesc = new HiveUtil().getTableDesc("ods_test", targetTable);
    for (int i = 0; i < targetTableDesc.size(); i++) {
      String[] fieldAndTypeSplit = targetTableDesc.get(i).split("\\s+");
      String selectField = fieldAndTypeSplit[0].toLowerCase();
      if (DEFAULT_FIELD.containsKey(selectField)) {
        selectField = DEFAULT_FIELD.get(selectField) + " AS " + selectField + ", -- " + fieldAndTypeSplit[1];
      } else {
        selectField = fieldAndTypeSplit[0] + ", -- " + fieldAndTypeSplit[1];
      }
      if (i == targetTableDesc.size() - 1) {
        selectField = selectField.replace(", -- ", " -- ");
      }
      sb.append(selectField).append("\n");
    }

    String streamSetJdbcSql = "select top 100000\n" +
            "CAST(CONVERT(DATE, " + TEMPLATE_FIELD.get("${derivativePartitionField}") + ") AS nvarchar(7)) AS hudi_pt,\n" +
            sb.toString() +
            "from "+targetTable.substring(0,targetTable.indexOf("_"))+"..[" + targetTable.substring(targetTable.indexOf("_") + 1) + "](NOLOCK)\n" +
            "WHERE ${offsetcolumn} > CAST(CAST(${OFFSET} AS BIGINT) AS rowversion) ORDER BY ${offsetcolumn}";
    String testSql = streamSetJdbcSql
            .replace("top 100000", "top 100")
            .replace("${offsetcolumn}", TEMPLATE_FIELD.get("${precombine.field}"))
            .replace("${OFFSET}", TEMPLATE_FIELD.get("${OFFSET}"));

    System.out.println("streamSet name,topic: ods." + targetTable);
    System.out.println("${OFFSET}:" + TEMPLATE_FIELD.get("${OFFSET}"));
    System.out.println("streamSetJdbcSql:\n" + streamSetJdbcSql );
    System.out.println();
    System.out.println(testSql);
  }


  public static void main(String[] args) throws IOException, URISyntaxException {
    jobTemplate();
    System.out.println("\n\n");
    sqlServerSelect();
  }


  static Map<String, String> DEFAULT_FIELD = new HashMap<>();
  static Map<String, String> TEMPLATE_FIELD = new HashMap<>();

  static {
    DEFAULT_FIELD.put("hrowkey", "'0'");
    DEFAULT_FIELD.put("record_timemillis", "0");
    DEFAULT_FIELD.put("sysevent", "'0'");
    DEFAULT_FIELD.put("modifytimestamp", "CAST(modifytimestamp AS BIGINT)");
  }

  static {
    TEMPLATE_FIELD.put("${targetTable}", "sq8flyt_dbo_orderparent_ct");
    TEMPLATE_FIELD.put("${tableType}", "COPY_ON_WRITE");
    TEMPLATE_FIELD.put("${recordkey.field}", "primaryid");
    TEMPLATE_FIELD.put("${partitionpath.field}", "hudi_pt");
    TEMPLATE_FIELD.put("${precombine.field}", "primaryid");
    TEMPLATE_FIELD.put("${derivativePartition}", "substr(updatetime,1,7)");

    TEMPLATE_FIELD.put("${derivativePartitionField}", TEMPLATE_FIELD.get("${derivativePartition}").replaceAll(".+\\((updatetime),.+", "$1"));
    TEMPLATE_FIELD.put("${OFFSET}", "1569577");
  }

  private static void jobTemplate() throws IOException, URISyntaxException {
    URL jobTemplate = StringReplace.class.getClassLoader().getResource("job_template.sh");
    String result = FileUtils.readFileToString(new File(jobTemplate.toURI()));

    for (String key : TEMPLATE_FIELD.keySet()) {
      result = result.replace(key, TEMPLATE_FIELD.get(key));
    }
    System.out.println("result:\n" + result);
  }

}
