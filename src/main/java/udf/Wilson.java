package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import static java.lang.Math.sqrt;

public class Wilson extends UDF {
    public Double evaluate(int num_pv,int num_click){
        if (num_pv * num_click == 0 || num_pv<num_click){
            return 0.;
        }
        double score = 0.f;
        double z = 1.96f;
        int n = num_pv;
        double p = 1.0f * num_click/num_pv;
        score = (p+z*z/(2.f*n)-z*sqrt((p*(1.0f-p)+z*z/(4.f*n))/n))/(1.f+z*z/n);
        return score;
    }
}
