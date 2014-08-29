package com.datatorrent.template;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import org.slf4j.LoggerFactory;

public class DoNothingOperator implements InputOperator, Partitioner<DoNothingOperator>, StatsListener
{
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DoNothingOperator.class);
  
  public final transient DefaultOutputPort<EventId> output = new DefaultOutputPort<EventId>();
  protected Queue<Long> batchIds = new LinkedList<Long>();
  protected Random random = new Random();
  protected long lastRepartition = 0;
  protected boolean firstRepartition = true;
  protected boolean secondRepartition = true;
  protected long emitCount = 0;
  protected long partitionId = 0;
  private transient long windowId;
  private transient int operatorId;
  
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
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    if(batchIds.isEmpty())
    {
      return;
    }
    
    EventId eventId = new EventId();
    long batchId = batchIds.poll();
    
    for(int i = 0;
        i < 1000;
        i++)
    {
      eventId.batchId = batchId;
      eventId.tupleId = i;
      eventId.windowId = windowId;
      eventId.operatorId = operatorId;
      eventId.partitionId = partitionId;
      
      emitCount++;
      output.emit(eventId);
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.operatorId = context.getId();
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public Collection<Partition<DoNothingOperator>> definePartitions(Collection<Partition<DoNothingOperator>> partitions, int incrementalCapacity)
  {
    this.lastRepartition = System.currentTimeMillis();
    long tempEmitTotal = 0;
    Queue<Long> totalBatchIds = new LinkedList<Long>();
    long partitionId = partitions.iterator().next().getPartitionedInstance().partitionId;
    partitionId++;
    
    if(!firstRepartition)
    {
      for(Partition<DoNothingOperator> partition: partitions)
      {
        tempEmitTotal += partition.getPartitionedInstance().emitCount;
        totalBatchIds.addAll(partition.getPartitionedInstance().batchIds);
      }
      
      Collections.sort((List<Long>) totalBatchIds);
    }
    else
    {
      for(long counter = 0L;
          counter < 2000L;
          counter++)
      {
        totalBatchIds.add(counter);
      }
    }
    
    int numOperators = 0;
    
    if(firstRepartition)
    {
      numOperators = 10;
    }
    else if(totalBatchIds.size() > 20)
    {
      numOperators = random.nextInt(10) + 1;
    }
    else if(totalBatchIds.size() <= 20)
    {
      numOperators = 1;
    }
    
    this.firstRepartition = false;
    int tempCounterTotal = totalBatchIds.size();
    long newCount = totalBatchIds.size() / numOperators;
    
    List<Partition<DoNothingOperator>> newPartitions = new ArrayList<Partition<DoNothingOperator>>();
    
    for(int counter = 0;
        counter < numOperators;
        counter++)
    {
      DoNothingOperator doNothingOperator = new DoNothingOperator();
      doNothingOperator.batchIds = new LinkedList<Long>();
      
      for(int batchCounter = 0;
          batchCounter < newCount;
          batchCounter++)
      {
        doNothingOperator.batchIds.add(totalBatchIds.poll());
      }
      
      if(counter == 0)
      {
        doNothingOperator.emitCount = tempEmitTotal;
        int remainder = tempCounterTotal % numOperators;
        
        for(int remainderCounter = 0;
            remainderCounter < remainder; 
            remainderCounter++)
        {
          doNothingOperator.batchIds.add(totalBatchIds.poll());
        }
        
        //doNothingOperator.counter += tempCounterTotal % numOperators;
      }
      
      doNothingOperator.lastRepartition = this.lastRepartition;
      doNothingOperator.firstRepartition = false;
      doNothingOperator.secondRepartition = secondRepartition;
      doNothingOperator.partitionId = partitionId;
      
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
