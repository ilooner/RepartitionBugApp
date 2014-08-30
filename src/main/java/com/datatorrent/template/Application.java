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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name = "RepartitionTestAppOriginal")
public class Application implements StreamingApplication
{
    private final Locality locality = null;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      dag.setAttribute(DAG.CHECKPOINT_WINDOW_COUNT, 1);
      EventEmitter doNothingOperator = dag.addOperator("donothing", new EventEmitter());
      dag.getOperatorMeta("donothing").getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 1);

      TupleCounter tupleCounter = dag.addOperator("counter", new TupleCounter());
      dag.getOperatorMeta("counter").getAttributes().put(OperatorContext.INITIAL_PARTITION_COUNT, 1);
      dag.getOperatorMeta("counter").getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 1);

      ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
      dag.getOperatorMeta("console").getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 1);

      EventWriter eventWriter = dag.addOperator("eventwriter", new EventWriter());
      dag.getOperatorMeta("eventwriter").getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 1);

      dag.addStream("donothingstream", doNothingOperator.output, tupleCounter.input).setLocality(locality);
      dag.addStream("counterstream", tupleCounter.counterOutput, console.input).setLocality(locality);
      dag.addStream("eventwriter", tupleCounter.eventOutput, eventWriter.input).setLocality(locality);
    }
}
