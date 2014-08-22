package com.datatorrent.template;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.slf4j.LoggerFactory;

public class DoNothingOperator implements InputOperator, Partitioner<DoNothingOperator>, StatsListener
{
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DoNothingOperator.class);
  
  public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
  protected long counter = 200;
  protected Random random = new Random();
  protected long lastRepartition = 0;
  protected boolean firstRepartition = true;
  protected boolean secondRepartition = true;
  protected long emitCount = 0;
    
  public DoNothingOperator()
  {
  }
  
  @Override
  public void emitTuples()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    if(counter == 0)
    {
      return;
    }
    
    counter--;
    
    for(int i = 0;
        i < 1000;
        i++)
    {
      emitCount++;
      output.emit(random.nextInt());
    }
    
    
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public Collection<Partition<DoNothingOperator>> definePartitions(Collection<Partition<DoNothingOperator>> partitions, int incrementalCapacity)
  {
    this.lastRepartition = System.currentTimeMillis();
    long tempCounterTotal = 0;
    long tempEmitTotal = 0;
    
    if(!firstRepartition)
    {
      for(Partition<DoNothingOperator> partition: partitions)
      {
        tempEmitTotal += partition.getPartitionedInstance().emitCount;
        tempCounterTotal += partition.getPartitionedInstance().counter;
      }
    }
    else
    {
      tempCounterTotal = 2000;
    }
    
    LOG.debug("TempCounterTotal: {}", tempCounterTotal);
    LOG.debug("Emit Count: {}", tempEmitTotal);
    
    long numOperators = 0;
    
    if(firstRepartition)
    {
      numOperators = 10;
    }
    else if(tempCounterTotal > 20)
    {
      numOperators = random.nextInt(10) + 1;
    }
    else if(tempCounterTotal <= 20)
    {
      numOperators = 1;
    }
    
    this.firstRepartition = false;
    long newCount = tempCounterTotal / numOperators;
    
    List<Partition<DoNothingOperator>> newPartitions = new ArrayList<Partition<DoNothingOperator>>();
    
    for(int counter = 0;
        counter < numOperators;
        counter++)
    {
      DoNothingOperator doNothingOperator = new DoNothingOperator();
      doNothingOperator.counter = newCount;
      
      if(counter == 0)
      {
        doNothingOperator.emitCount = tempEmitTotal;
        doNothingOperator.counter += tempCounterTotal % numOperators;
      }
      
      doNothingOperator.lastRepartition = this.lastRepartition;
      doNothingOperator.firstRepartition = false;
      doNothingOperator.secondRepartition = secondRepartition;
    
      newPartitions.add(new DefaultPartition<DoNothingOperator>(doNothingOperator));
    }
    
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<DoNothingOperator>> partitions)
  {
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    if(System.currentTimeMillis() - 60 * 1000 > lastRepartition)
    {
      Response response = new Response();
      response.repartitionRequired = true;
      return response;
    }
    
    return new Response();
  }
}
