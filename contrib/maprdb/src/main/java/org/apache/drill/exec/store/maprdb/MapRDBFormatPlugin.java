package org.apache.drill.exec.store.maprdb;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MapRDBFormatPlugin implements FormatPlugin{

	private static final 
	@Override
	public boolean supportsRead() {
		// TODO Auto-generated method stubs
		return true;
	}

	@Override
	public boolean supportsWrite() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FormatMatcher getMatcher() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractWriter getWriter(PhysicalOperator child, String location)
			throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public AbstractGroupScan getGroupScan(FileSelection selection)
			throws IOException {
		// TODO Auto-generated method stub
		List<String> files = selection.getAsFiles();
		assert(files.size() == 1);
		String tableName = files.get(0);
		HBaseScanSpec scanSpec = new HBaseScanSpec(tableName);
	    return new MapRDBGroupScan(this, scanSpec, null);
	}

	@Override
	public Set<StoragePluginOptimizerRule> getOptimizerRules() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractGroupScan getGroupScan(FileSelection selection,
			List<SchemaPath> columns) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FormatPluginConfig getConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StoragePluginConfig getStorageConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DrillFileSystem getFileSystem() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DrillbitContext getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
