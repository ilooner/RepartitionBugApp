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
  //public static final String FILE_NAME = "/user/tim/repartitionTest";
  private int operatorId;
  private long windowId;

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
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    super.setup(context);
  }

  @Override
  public Path nextFilePath()
  {
    Path file = new Path(super.filePath, operatorId + "-" + Long.toHexString(windowId));
    
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
