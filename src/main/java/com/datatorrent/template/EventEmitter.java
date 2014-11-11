package com.datatorrent.template;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;

public class EventEmitter implements InputOperator, Partitioner<EventEmitter>, StatsListener
{
  private static final Logger logger = LoggerFactory.getLogger(EventEmitter.class);

  public final transient DefaultOutputPort<EventId> output = new DefaultOutputPort<EventId>()
  {
    @Override
    public Unifier<EventId> getUnifier()
    {
      return (new EventEmitterUnifier());
    }
  };
  protected Queue<Long> batchIds = new LinkedList<Long>();
  protected ArrayList<Long> processed = new ArrayList<Long>();
  protected Random random = new Random();
  protected long lastRepartition = 0;
  protected boolean firstRepartition = true;
  protected boolean secondRepartition = true;
  protected long emitCount = 0;
  protected long partitionId = 0;
  private long windowId;
  private transient int operatorId;
  private long activationWindowId;

  @Override
  public void emitTuples()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    logger.info("processed events {}", emitCount);
    logger.info("prev windowId = {}, windowId = {}", this.windowId, windowId);
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    if (batchIds.isEmpty()) {
      //Operator.Util.shutdown();
      return;
    }

    EventId eventId = new EventId();
    long batchId = batchIds.poll();

    for (int i = 0;
         i < 1000;
         i++) {
      eventId.batchId = batchId;
      eventId.tupleId = i;
      eventId.windowId = windowId;
      eventId.operatorId = operatorId;
      eventId.partitionId = partitionId;
      eventId.recieveWindowId = activationWindowId;

      emitCount++;
      output.emit(eventId);
    }

    processed.add(batchId);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    activationWindowId = context.getValue(OperatorContext.ACTIVATION_WINDOW_ID);
    if (windowId == 0) {
      windowId = activationWindowId;
    }
    else {
      logger.info("activation window Id = {}", context.getValue(OperatorContext.ACTIVATION_WINDOW_ID));
      assert (windowId == context.getValue(OperatorContext.ACTIVATION_WINDOW_ID));
    }
    this.operatorId = context.getId();
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public Collection<Partition<EventEmitter>> definePartitions(Collection<Partition<EventEmitter>> partitions, int incrementalCapacity)
  {
    this.lastRepartition = System.currentTimeMillis();
    long tempEmitTotal = 0;
    Queue<Long> totalBatchIds = new LinkedList<Long>();

    long partitionId = partitions.iterator().next().getPartitionedInstance().partitionId;
    partitionId++;

    if (!firstRepartition) {
      for (Partition<EventEmitter> partition : partitions) {
        tempEmitTotal += partition.getPartitionedInstance().emitCount;
        totalBatchIds.addAll(partition.getPartitionedInstance().batchIds);
        processed.addAll(partition.getPartitionedInstance().processed);
      }

      Collections.sort((List<Long>) totalBatchIds);
    }
    else {
      for (long counter = 0L;
           counter < 2000L;
           counter++) {
        totalBatchIds.add(counter);
      }
    }
    int numOperators = 0;

    if (firstRepartition) {
      numOperators = 10;
    }
    else if (totalBatchIds.size() > 20) {
      numOperators = random.nextInt(10) + 2;
    }
    else if (totalBatchIds.size() <= 20) {
      numOperators = 1;
    }

    logger.info("called repartition at windowId {} and total emitted tuples {} ", partitions.iterator().next().getPartitionedInstance().windowId, tempEmitTotal);
    logger.info("new partitions {}",numOperators);

    this.firstRepartition = false;
    int tempCounterTotal = totalBatchIds.size();
    long newCount = totalBatchIds.size() / numOperators;

    List<Partition<EventEmitter>> newPartitions = new ArrayList<Partition<EventEmitter>>();

    for (int counter = 0; counter < numOperators; counter++) {
      EventEmitter emitter = new EventEmitter();
      emitter.batchIds = new LinkedList<Long>();

      for (int batchCounter = 0;
           batchCounter < newCount;
           batchCounter++) {
        emitter.batchIds.add(totalBatchIds.poll());
      }

      if (counter == 0) {
        emitter.emitCount = tempEmitTotal;
        int remainder = tempCounterTotal % numOperators;

        for (int remainderCounter = 0;
             remainderCounter < remainder;
             remainderCounter++) {
          emitter.batchIds.add(totalBatchIds.poll());
        }
      }

      emitter.lastRepartition = this.lastRepartition;
      emitter.firstRepartition = false;
      emitter.secondRepartition = secondRepartition;
      emitter.partitionId = partitionId;
      newPartitions.add(new DefaultPartition<EventEmitter>(emitter));
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<EventEmitter>> partitions)
  {
    logger.info("processed = {}", processed);
    for (Partition<EventEmitter> p : partitions.values()) {
      logger.info("tobeprocessed = {}", p.getPartitionedInstance().batchIds);
    }

    for (long l : processed) {
      for (Partition<EventEmitter> p : partitions.values()) {
        for (long b : p.getPartitionedInstance().batchIds) {
          assert (b != l);
        }
      }
    }
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    if (System.currentTimeMillis() - 60 * 1000 > lastRepartition) {
      Response response = new Response();
      response.repartitionRequired = true;
      return response;
    }
    return new Response();
  }

  @Override
  public String toString()
  {
    return "EventEmitter{" + "batchIds=" + batchIds + ", emitCount=" + emitCount + ", partitionId=" + partitionId + ", windowId=" + windowId + '}';
  }

}
