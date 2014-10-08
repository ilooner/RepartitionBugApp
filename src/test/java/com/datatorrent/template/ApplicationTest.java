/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.template;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication() throws Exception
  {
    File testDir = new File("target/testdata");
    FileUtils.deleteDirectory(testDir);
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.eventwriter.prop.filePath", testDir.getPath());
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new Application(), conf);    
    LocalMode.Controller lc = lma.getController();
    lc.run();
  }

}
