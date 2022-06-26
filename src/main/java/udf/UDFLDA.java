package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

@Description(name = "lda",
    value = "_FUNC_(words,topics,initial,alpha,num_iterations) - " +
            " Perform LDA inference on a document given by 0-indexed" +
            " words using the topics (which should be properly " +
            "normalized and smoothed. Returns the topic propertions.")
public class UDFLDA extends UDF {
    public ArrayList<Double> evaluate(
            ArrayList<Integer> words,
            ArrayList<Double> topics,
            ArrayList<Double> initial,
            Double alpha,
            Integer num_iterations
    ) {
        if (words == null || topics == null || initial == null ||
            alpha == null || num_iterations == null){
            return null;
        }
        int K = initial.size();
        int Nw = words.size();
        if (K == 0 || num_iterations <= 0){
            return null;
        }

        double[] document_sum = new double[K];
        double[] assignments = new double[K*Nw];
        for (int i = 0; i < K; i++) {
            document_sum[i] = initial.get(i)*Nw;
            for (int j = 0; j < Nw; j++) {
                assignments[i+j*K]=initial.get(i);
            }
        }
        for (int i = 0; i < num_iterations; i++) {
            for (int j = 0; j < Nw; j++) {
                int word = words.get(j);
                double w_sum = 0.0;
                for (int k = 0; k < K; k++) {
                    document_sum[k]-=assignments[k+j*K];
                }
                for (int k = 0; k < K; k++) {
                    assignments[k+j*K]=(document_sum[k]+alpha)*topics.get(k+word*K);
                    w_sum+=assignments[k+j*K];
                }
                for (int k = 0; k < K; k++) {
                    assignments[k+j*K]/=w_sum;
                    document_sum[k]+=assignments[k+j*K];
                }
            }
        }

        double sum = 0.0;
        for (int i = 0; i < K; i++) {
            sum += document_sum[i];
        }

        ArrayList<Double> result = new ArrayList<>(K);
        for (int i = 0; i < K; i++) {
            if (sum==0.0){
                result.add(1.0/K);
            } else {
                result.add(document_sum[i]/sum);
            }
        }
        return result;
    }

}
