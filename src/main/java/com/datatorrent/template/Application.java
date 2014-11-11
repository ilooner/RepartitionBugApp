/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.template;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name = "RepartitionTestAppOriginal")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.CHECKPOINT_WINDOW_COUNT, 1);
    EventEmitter emitter = dag.addOperator("Emitter", new EventEmitter());
    TupleCounter tupleCounter = dag.addOperator("Counter", new TupleCounter());
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());
    EventWriter eventWriter = dag.addOperator("Writer", new EventWriter());

    dag.addStream("Events", emitter.output, tupleCounter.input);
    dag.addStream("Counts", tupleCounter.counterOutput, console.input);
    dag.addStream("Persist", tupleCounter.eventOutput, eventWriter.input);
    dag.setInputPortAttribute(tupleCounter.input, Context.PortContext.STREAM_CODEC, new KryoSerializableStreamCodec<Object>());
    dag.setInputPortAttribute(console.input, Context.PortContext.STREAM_CODEC, new KryoSerializableStreamCodec<Object>());
    dag.setInputPortAttribute(eventWriter.input, Context.PortContext.STREAM_CODEC, new KryoSerializableStreamCodec<Object>());

  }
}
