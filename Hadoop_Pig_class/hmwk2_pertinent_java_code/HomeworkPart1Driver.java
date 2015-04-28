/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ucsc.hadoop;

import org.apache.hadoop.util.ProgramDriver;

import ucsc.hadoop.homework2.ActorList;
import ucsc.hadoop.homework2.MovieCountPerActor;

/**
 * Driver to fire off the ActorList mapreduce.  
 * 
 * Should use the corresponding run configuration
 * Output appears in project's output folder
 * 
 * @author john
 */
public class HomeworkPart1Driver {
  
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
    	System.out.println("argv[[0]: "+argv[0]);
    	pgd.addClass("actorlist", ActorList.class, 
                "A map/reduce program that lists each major actor in a movie");
    	pgd.driver(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}
	
