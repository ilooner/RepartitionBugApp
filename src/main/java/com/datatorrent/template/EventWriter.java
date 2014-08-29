package com.datatorrent.template;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFSDirectoryInputOperator;
import com.datatorrent.lib.io.fs.AbstractHdfsFileOutputOperator;
import com.datatorrent.lib.io.fs.AbstractHdfsRollingFileOutputOperator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

public class EventWriter extends AbstractHdfsRollingFileOutputOperator<EventId>
{
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EventWriter.class);
  public static final String FILE_NAME = "hdfs://node2.morado.com/user/tim/repartitionTest";
  private int operatorId;
  private long windowId;
  
  @Override
  protected void processTuple(EventId t)
  {
    try
    {
      if(bufferedOutput == null)
      {
        openFile(new Path(FILE_NAME));
      }
    
      bufferedOutput.write(getBytesForTuple(t));
    }
    catch(Exception e)
    {
    }
  }

  @Override
  protected byte[] getBytesForTuple(EventId t)
  {
    return t.toString().getBytes();
  }
  
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;
  }
  
  @Override
  public void endWindow()
  {
    super.endWindow();
    
    try {
      closeFile();
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    super.setup(context);
  }

  @Override
  public Path nextFilePath()
  {
    Path file = new Path(FILE_NAME, operatorId + "-" + windowId);
    
    try
    {
      if(fs.exists(file))
      {
        fs.delete(file, true);
        LOG.info("Delete file {}", file.toString());
      }
    }
    catch(IOException e)
    {
      throw new RuntimeException(e);
    }
    
    return file;
  }
}
