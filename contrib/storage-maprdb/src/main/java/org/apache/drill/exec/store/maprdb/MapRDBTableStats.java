package org.apache.drill.exec.store.maprdb;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.mapr.fs.hbase.HBaseAdminImpl;
import org.apache.hadoop.hbase.client.mapr.BaseTableMappingRules;
import org.apache.hadoop.hbase.client.mapr.TableMappingRulesFactory;

public class MapRDBTableStats {
	
	private int numRows;
	
	public MapRDBTableStats(HTable table) throws IOException {
		HBaseConfiguration config = HBaseConfiguration.create();
		admin = new HBaseAdminImpl(config, TableMappingRulesFactory.create(config));
		numRows = admin.getNumRows(table.getTableName());
	}

	public int getNumRows() {
		return numRows;
	}
	
}
