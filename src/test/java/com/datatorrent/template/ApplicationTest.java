/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.template;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      new Application().populateDAG(lma.getDAG(), new Configuration(false));
      LocalMode.Controller lc = lma.getController();
      lc.run();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
