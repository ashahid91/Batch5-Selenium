package com.fepoc.test.solr;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.json.JSONArray;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fepoc.test.util.GlobalParameters;

public class OPLPDVAudit extends SolrResearchClass {
	private static List<String> noRecordFound = new ArrayList<>();
	private static List<String> missingOPL = new ArrayList<>();
	private static final Logger LOGGER = Logger.getLogger(OPLPDVAudit.class.getName());

	public static void oplAudit() {

		connection = getDB2Connection();
		HBaseConnect.conn = HBaseConnect.connToHBase();
		System.out.println("Enviornment: " + GlobalParameters.reqRegion);
		String sql = "WITH DeltaOPL AS (\r\n" + " SELECT  distinct contract_id, member_id , OPL_ID\r\n"
				+ " FROM FEPC2.OTR_PRTY_LIABILITY\r\n" + " WHERE  LAST_PROC_TS > current_timestamp - 1 days \r\n"
				+ "),\r\n" + "FILTEREDDATA  AS (\r\n" + "SELECT distinct contract_id, member_id\r\n"
				+ " FROM FEPC2.MEMBER_ELIGIBILITY me\r\n"
				+ " where contract_id IN (SELECT contract_id FROM DeltaOPL where CONTRACT_ID != 0)\r\n"
				+ "   OR member_id IN (SELECT member_id from DeltaOPL where member_id !=0)\r\n" + ")\r\n"
				+ "SELECT  f.contract_id, f.member_id, OPL_ID\r\n" + "FROM FILTEREDDATA f\r\n"
				+ "LEFT JOIN DeltaOPL d\r\n" + "ON  f.CONTRACT_ID = d.CONTRACT_ID OR f.MEMBER_ID = d.MEMBER_ID ";
		List<Map<String, Object>> list = new ArrayList<>();
		MapListHandler beanListHandler = new MapListHandler();
		QueryRunner runner = new QueryRunner();
		try {
			list = runner.query(connection, sql, beanListHandler);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		Instant jobStartTime = Instant.now();
		System.out.println(list.size());
		processDelta(list);
		if (!CollectionUtils.isEmpty(failedRecList)) {
			String fileName = System.getProperty("user.dir") + "/test-output/data-validation/"
					+ GlobalParameters.reqRegion + "-Failed-" + sdf.format(new Date()) + ".csv";
			try (FileWriter fw = new FileWriter(fileName)) {
				fw.write("id,field_name,db2_val,hbase_val\n");
				for (FailedRecord rec : failedRecList) {

					fw.write(rec.toString());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			LOGGER.info("Created unmatched report: " + fileName);
			failedRecList.clear();
		}
		if (!noRecordFound.isEmpty()) {
			String fName = System.getProperty("user.dir") + "/test-output/data-validation/"
					+ GlobalParameters.reqRegion + "-Failedrowkey-" + sdf.format(new Date()) + ".csv";
			saveAsCsv(noRecordFound, fName);
		}
		if (!missingOPL.isEmpty()) {
			String fName = System.getProperty("user.dir") + "/test-output/data-validation/"
					+ GlobalParameters.reqRegion + "-FailedoplId-" + sdf.format(new Date()) + ".csv";
			saveAsCsv(missingOPL, fName);
		}

		System.out.println("========================================================");
		System.out.println("	Number of Records Validated: " + list.size());
		System.out.println("	Number of Records Missing Row Key: " + noRecordFound.size());
		System.out.println("	Number of Records Missing OPL ID: " + missingOPL.size());
		long second = Duration.between(jobStartTime, Instant.now()).getSeconds();
		double minutes = second/60;
		System.out.println("	JOB EXECUTION TIME: " + minutes + " minutes");
		System.out.println("========================================================");

	}

	private static void processDelta(List<Map<String, Object>> list) {
		AtomicInteger recordNumber = new AtomicInteger(1);

		list.parallelStream().forEach(map -> {
			System.out.println(recordNumber + "of size " + list.size());
			String contractID = map.get("contract_id").toString();
			String rowKey = contractID.charAt(contractID.length() - 1) + "_" + contractID;
			String oplId = map.get("opl_id").toString();
			Map<String, String> hbaseMap = HBaseConnect.queryHbaseData("fepenrl_c2:OPL", rowKey);
			if (hbaseMap == null || hbaseMap.isEmpty()) {
				noRecordFound.add(rowKey);
			} else {
				for (Entry<String, String> entry : hbaseMap.entrySet()) {
					String key = entry.getKey();
					String val = entry.getValue();
					if (key.equals("MBR_OPL")) {
						if (val.startsWith("[")) {
							extractMemberOPLField(hbaseMap, oplId);
						}
					}
				}

			}
			recordNumber.incrementAndGet();

		});

	}

	private static void extractMemberOPLField(Map<String, String> hbaseMap, String oplId) {
		for (Entry<String, String> entry : hbaseMap.entrySet()) {
			String key = entry.getKey();
			String val = entry.getValue();
			if (key.equals("MBR_OPL")) {
				if (val.startsWith("[")) {
					List<Map<String, String>> hbasedataSet = convertJSONToMap(val);
					extractedOPLRecord(hbasedataSet, oplId);
				}
			}
		}

	}

	/**
	 * @param val
	 * @return
	 */
	private static List<Map<String, String>> convertJSONToMap(String val) {
		JSONArray arr = new JSONArray(val);
		List<Map<String, String>> hbasedataSet = new ArrayList<>();
		List jsonList = new ArrayList<>();
		for (int i = 0; i < arr.length(); i++) {
			jsonList.add(arr.getJSONObject(i));
		}
		for (int i = 0; i < jsonList.size(); i++) {
			HashMap<String, String> map1;
			try {
				map1 = new ObjectMapper().readValue(jsonList.get(i).toString(), HashMap.class);
				hbasedataSet.add(map1);
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return hbasedataSet;
	}

	/**
	 * @param hbasedataSet
	 */
	private static void extractedOPLRecord(List<Map<String, String>> hbasedataSet, String oplID) {
		Optional<Map<String, String>> desiredOPLIDMap = hbasedataSet.stream()
				.filter(map -> oplID.equals(map.get("OPL_ID"))).findFirst();
		MapHandler beanListHandler = new MapHandler();
		QueryRunner runner = new QueryRunner();
		String otr_prty_liability = "SELECT OPL_ID, case when CONTRACT_ID = 0 THEN '' ELSE CHAR(CONTRACT_ID) END AS CNTR_ID, MEMBER_ID AS MBR_ID, OPL_TYPE AS\r\n"
				+ "     OPL_TYP_CD, OPL_STATUS AS OPL_STAT_CD, OPL_REASON AS OPL_RSN_CD,\r\n"
				+ "     OPL_SOURCE AS OPL_SRC_CD, OPL_RECORD AS OPL_REC_TYP_CD, PLAN_CODE AS\r\n"
				+ "     PLN_CD, TRIM (POLICY_ID) AS PLCY_ID, EFFECTIVE_DATE AS EFF_DT, \r\n"
				+ "     TERM_DATE AS TRM_DT, LAST_PROC_DATE AS\r\n"
				+ "     LAST_PROC_DT, EXPLANATION_TXT AS EXPLNTN_TXT, MDCR_SUPPT_DOC_IND FROM FEPC2.otr_prty_liability WHERE OPL_ID = "
				+ oplID;
		String other_coverage = "SELECT \r\n"
				+ "   carrier_id  AS carr_id,coverage_level AS cvrg_lvl, birth_date as bth_dt,\r\n"
				+ "  sex, trim(first_name) AS fst_nm, trim(last_name) AS last_nm,\r\n"
				+ "  dpnt_cvrg_code AS dpndt_cvrg_cd, dpnt_cvrg_reason AS dpnt_cvrg_rsn_cd,\r\n"
				+ "  trim(employer_name) AS emplr_nm\r\n" + "FROM fepc2.other_coverage where opl_id =" + oplID;
		List<String> sqlQueries = Arrays.asList(otr_prty_liability, other_coverage);
		sqlQueries.parallelStream().forEach(query -> {
			try {
				Map<String, Object> db2Map = runner.query(connection, query, beanListHandler);
				if (db2Map != null && !db2Map.isEmpty()) {
					if (desiredOPLIDMap.isPresent()) {
						Map<String, Object> oplMap = new HashMap<>(desiredOPLIDMap.get());
						DB2SolrCompare.display("OPL", db2Map.keySet(), db2Map, oplMap, oplID, "HBase");
					} else {
						missingOPL.add(oplID);
						System.out.println("OPL ID was not found");
					}
				}
			} catch (SQLException e) {
				System.out.println("OPL ID is not found in db2");
				e.printStackTrace();
			}

		});

	}

}
