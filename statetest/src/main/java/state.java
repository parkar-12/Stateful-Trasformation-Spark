import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import java.io.Serializable;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import java.util.*;

/***
 *This code signifies stateful transformation in spark using structured streaming (can be used for batch queries as well)
 * Every state has two fields :- state name and state measure
 * Statename:- Name of the current state
 * Measure:- A numeric value of particular key for that state
 * Run the code :- spark-submit jar_filename
 */

public  class state {

    public static void main(String[] args) throws StreamingQueryException {
        /***
         * MapGroupsWithStateFunction is used for updating the state for a particular key.
         */
        MapGroupsWithStateFunction<String, Row, StateInfo, StateUpdate> updateEvents=
                new MapGroupsWithStateFunction<String, Row, StateInfo, StateUpdate>() {
                    @Override
                    public StateUpdate call(String key, Iterator<Row> values, GroupState<StateInfo> state) throws Exception {

                        int measure=0;
                        StateInfo updatedState = new StateInfo();
                        String name="first";

                        /***
                         * If a state doesn't exist(at the initial state), then a state is created with initial values
                         * State name = first
                         * State measure = 0
                         */

                        if(!state.exists()){
                            updatedState.setStatename(name);
                            updatedState.setMeasure(measure);
                            state.update(updatedState);
                        }

                         StateInfo oldstate = state.get();
                         name = oldstate.getStatename();
                         measure = oldstate.getMeasure();

                        while(values.hasNext()){
                            Row temp = values.next();
                            String alpha = temp.getString(1);
                            int temp_measure = temp.getInt(2);
                            System.out.println(alpha+" "+name+" "+temp_measure);
                            if(name.equals("first") && alpha.equals("a")){
                              //  System.out.println("Enter 1");
                                name = "second";
                                measure = measure + temp_measure;
                            }
                            else if(name.equals("first") && alpha.equals("b")){
                               // System.out.println("Enter 2");
                                name = "third";
                                measure = measure - temp_measure;
                            }
                            else if(name.equals("first") && alpha.equals("c")){
                                // System.out.println("Enter 3");
                                name = "third";
                            }
                            else if(name.equals("second") && alpha.equals("a")){
                                // System.out.println("Enter 4");
                                name = "second";
                                measure = measure + temp_measure;
                            }
                            else if(name.equals("second") && alpha.equals("b")){
                                // System.out.println("Enter 5");
                                name = "third";
                                measure = measure - temp_measure;
                            }
                            else if(name.equals("second") && alpha.equals("c")){
                                // System.out.println("Enter 6");
                                name = "fourth";
                            }
                            else if(name.equals("third") && alpha.equals("a")){
                                // System.out.println("Enter 7");
                                name = "third";
                                measure = measure + temp_measure;
                            }
                            else if(name.equals("third") && alpha.equals("b")){
                                // System.out.println("Enter 8");
                                name = "third";
                                measure = measure - temp_measure;
                            }
                            else if(name.equals("third") && alpha.equals("c")){
                                // System.out.println("Enter 9");
                                name = "fourth";
                            }
                            else if(name.equals("fourth")){
                                return new StateUpdate(
                                        key, state.get().getStatename(), state.get().getMeasure());
                            }
                        }
                        System.out.println(name+" "+measure);
                        updatedState.setStatename(name);
                        updatedState.setMeasure(measure);
                        state.update(updatedState);
                        return new StateUpdate(
                                key, state.get().getStatename(), state.get().getMeasure());
                    }
                };


        SparkSession spark = SparkSession.builder().appName("random").config("spark.master", "local").getOrCreate();
        String schema = "id STRING,alphabet STRING, measure INT";
        Dataset<Row> df = spark.readStream().schema(schema).option("header",true).csv("state/");
        // Dataset<Row> df = spark.read().schema(schema).option("header",true).csv("state/*"); for batch queries
        Dataset<StateUpdate> val = df.groupByKey(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                return value.getString(0);
            }
        }, Encoders.STRING()).mapGroupsWithState(updateEvents,
                Encoders.bean(StateInfo.class),
                Encoders.bean(StateUpdate.class),
                GroupStateTimeout.NoTimeout());
        /***
         * for batch queries
         * val.show()
         * val.write().format("console")
         */

        StreamingQuery query = val.filter("statename = 'fourth'")
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();

    }

    /***
     * StateInfo class is used to set and retrieve the state variables - statename and measure
     */
    public static class StateInfo implements Serializable {
        private int measure = 0;
        private String statename;


        public int getMeasure() {
            return measure;
        }

        public void setMeasure(int measure) {
            this.measure = measure;
        }

        public String getStatename() {
            return statename;
        }

        public void setStatename(String statename) {
            this.statename = statename;
        }

    }

    /***
     * State update class is used for updating of state for the particular key(In this case id)
     */

    public static class StateUpdate implements Serializable {
        private String id;
        private String statename;
        private int measure;

        public StateUpdate() {
        }

        public StateUpdate(String id, String statename, int measure) {
            this.id = id;
            this.statename = statename;
            this.measure = measure;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getStatename() {
            return statename;
        }

        public void setStatename(String statename) {
            this.statename = statename;
        }

        public int getMeasure() {
            return measure;
        }

        public void setMeasure(int measure) {
            this.measure = measure;
        }

    }
}
