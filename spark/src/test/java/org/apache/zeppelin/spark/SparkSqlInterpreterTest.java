/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.spark;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SparkSqlInterpreterTest {

	private SparkSqlInterpreter sql;
  private SparkInterpreter repl;
  private InterpreterContext context;
  private InterpreterGroup intpGroup;

	@Before
	public void setUp() throws Exception {
		Properties p = new Properties();

		if (repl == null) {

		  if (SparkInterpreterTest.repl == null) {
		    repl = new SparkInterpreter(p);
		    repl.open();
		    SparkInterpreterTest.repl = repl;
		  } else {
		    repl = SparkInterpreterTest.repl;
		  }

  		sql = new SparkSqlInterpreter(p);

  		intpGroup = new InterpreterGroup();
		  intpGroup.add(repl);
		  intpGroup.add(sql);
		  sql.setInterpreterGroup(intpGroup);
		  sql.open();
		}
		context = new InterpreterContext("id", "title", "text", new HashMap<String, Object>(), new GUI(),
		    new AngularObjectRegistry(intpGroup.getId(), null),
		    new LinkedList<InterpreterContextRunner>());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		repl.interpret("case class Test(name:String, age:Int)", context);
		repl.interpret("val test = sc.parallelize(Seq(Test(\"moon\", 33), Test(\"jobs\", 51), Test(\"gates\", 51), Test(\"park\", 34)))", context);
		repl.interpret("test.registerAsTable(\"test\")", context);

		InterpreterResult ret = sql.interpret("select name, age from test where age < 40", context);
		assertEquals(InterpreterResult.Code.SUCCESS, ret.code());
		assertEquals(Type.TABLE, ret.type());
		assertEquals("name\tage\nmoon\t33\npark\t34\n", ret.message());

	  assertEquals(InterpreterResult.Code.ERROR, sql.interpret("select wrong syntax", context).code());
		assertEquals(InterpreterResult.Code.SUCCESS, sql.interpret("select case when name==\"aa\" then name else name end from people", context).code());
	}

	@Test
	public void testStruct(){
		repl.interpret("case class Person(name:String, age:Int)", context);
		repl.interpret("case class People(group:String, person:Person)", context);
		repl.interpret("val gr = sc.parallelize(Seq(People(\"g1\", Person(\"moon\",33)), People(\"g2\", Person(\"sun\",11))))", context);
		repl.interpret("gr.registerAsTable(\"gr\")", context);
		InterpreterResult ret = sql.interpret("select * from gr", context);
		assertEquals(InterpreterResult.Code.SUCCESS, ret.code());
	}

	@Test
	public void test_null_value_in_row() {
		repl.interpret("import org.apache.spark.sql._", context);
		repl.interpret("def toInt(s:String): Any = {try { s.trim().toInt} catch {case e:Exception => null}}", context);
		repl.interpret("val schema = StructType(Seq(StructField(\"name\", StringType, false),StructField(\"age\" , IntegerType, true),StructField(\"other\" , StringType, false)))", context);
		repl.interpret("val csv = sc.parallelize(Seq((\"jobs, 51, apple\"), (\"gates, , microsoft\")))", context);
		repl.interpret("val raw = csv.map(_.split(\",\")).map(p => Row(p(0),toInt(p(1)),p(2)))", context);
		repl.interpret("val people = z.sqlContext.applySchema(raw, schema)", context);
		repl.interpret("people.registerTempTable(\"people\")", context);

		InterpreterResult ret = sql.interpret("select name, age from people where name = 'gates'", context);
		System.err.println("RET=" + ret.message());
		assertEquals(InterpreterResult.Code.SUCCESS, ret.code());
		assertEquals(Type.TABLE, ret.type());
		assertEquals("name\tage\ngates\tnull\n", ret.message());
	}
}
