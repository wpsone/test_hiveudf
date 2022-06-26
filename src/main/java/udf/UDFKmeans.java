package udf;

import antlr.SemanticException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;

import java.util.ArrayList;

@Description(name = "kmeans",
    value = "_FUNC_(points,K,max_iterations) - Perform K-means clustering on a collection on a collection of points represented as arrays and  returns an array of cluster centers." )
public class UDFKmeans extends UDF {
    public int sample(double[] weights) throws SemanticException {
        double weight_sum = 0.0;
        for (int i = 0; i < weights.length; ++i) {
            if (weights[i]<0.0){
                return -3;
            }
            weight_sum += weights[i];
        }
        double r = Math.random();
        if (weight_sum == 0.0){
            return (int)(r*weights.length);
        }
        for (int i = 0; i < weights.length; ++i) {
            if (r<weights[i]/weight_sum){
                return i;
            }
            r -= weights[i]/weight_sum;
        }
        return -2;
    }

    public double squared_dist(ArrayList<Double> center,ArrayList<Double> point) throws UDFArgumentTypeException {
        int M = point.size();
        if (M!=center.size()){
            throw new UDFArgumentTypeException(M,"This should never happen.");
        }
        double dist2 = 0;
        for (int i = 0; i < M; ++i) {
            dist2 += (center.get(i)-point.get(i))*(center.get(i)-point.get(i));
        }
        return dist2;
    }

    public ArrayList<ArrayList<Double>> evaluate(ArrayList<ArrayList<Double>> points,Integer K,Integer max_iterations) throws UDFArgumentTypeException, SemanticException {
        if (K == null || max_iterations == null || points == null){
            return null;
        }
        if (K <= 0){
            throw new UDFArgumentTypeException(K,"K should be positvie.");
        }

        int N = points.size();
        if (N < K){
            for (int i = 0; i < N; ++i) {
                points.get(i).add(0.0);
                points.get(i).add(1.0);
            }
            return points;
        }

        int M = points.get(0).size();
        ArrayList<ArrayList<Double>> centers = new ArrayList<>();
        double dist2s[] = new double[N];
        for (int i = 0; i < K; i++) {
            int new_center = -1;
            if (i>0){
                for (int j = 0; j < N; ++j) {
                    dist2s[j] = 1e100;
                    ArrayList<Double> point = points.get(j);
                    if (point.size()!=M){
                        throw new UDFArgumentTypeException(M,"Size of tuples do not match.");
                    }
                    for (int k = 0; k < i; ++k) {
                        double dist2 = squared_dist(centers.get(k),point);
                        if (dist2 < dist2s[j]){
                            dist2s[j] = dist2;
                        }
                    }
                }
                new_center = sample(dist2s);
            } else {
                new_center = (int) (Math.random()*N);
            }

            ArrayList<Double> point = points.get(new_center);
            if (point.size() != M){
                throw new UDFArgumentTypeException(M,"Size of tuples do not match.");
            }
            ArrayList<Double> new_point = new ArrayList<>();
            for (int j = 0; j < M; ++j) {
                new_point.add(point.get(j).doubleValue());
            }
            centers.add(new_point);
        }

        int[] assignments = new int[N];
        int[] center_counts = new int[K];
        for (int i = 0; i < N; i++) {
            assignments[i]=-1;
        }
        for (int i = 0; i < max_iterations; ++i) {
            for (int j = 0; j < K; ++j) {
                center_counts[j]=0;
            }
            boolean changed = false;
            for (int j = 0; j < N; ++j) {
                int old_assignment = assignments[i];
                ArrayList<Double> point = points.get(i);
                double mindist2 = 1e100;
                for (int k = 0; k < K; ++k) {
                    double dist2 = squared_dist(centers.get(j),point);
                    if (dist2 < mindist2){
                        assignments[i]=j;
                        mindist2 = dist2;
                    }
                }
                if (assignments[j]!=old_assignment){
                    changed=true;
                }
                center_counts[assignments[j]]++;
            }
            if (!changed){
                break;
            }

            for (int j = 0; j < K; ++j) {
                for (int k = 0; k < M; ++k) {
                    centers.get(j).set(k,0.0);
                }
            }
            for (int j = 0; j < N; ++j) {
                ArrayList<Double> point = points.get(j);
                ArrayList<Double> center = centers.get(assignments[j]);
                for (int k = 0; k < M; ++k) {
                    center.set(k,center.get(k)+point.get(k)/center_counts[assignments[j]]);
                }
            }
        }

        ArrayList<ArrayList<Double>> extra_stats = new ArrayList<>();
        for (int i = 0; i < K; ++i) {
            ArrayList<Double> extra_stat = new ArrayList<>();
            extra_stat.add(0.0);
            extra_stat.add(0.0);
            extra_stats.add(extra_stat);
        }

        for (int i = 0; i < N; ++i) {
            ArrayList<Double> point = points.get(i);
            ArrayList<Double> center = centers.get(assignments[i]);
            ArrayList<Double> extra_stat = extra_stats.get(assignments[i]);
            extra_stat.set(1,extra_stat.get(1)+1);
            extra_stat.set(0,extra_stat.get(0)+squared_dist(center,point));
        }

        for (int i = 0; i < K; ++i) {
            double num_points = extra_stats.get(i).get(1);
            centers.get(i).add(extra_stats.get(i).get(0)/num_points);
            centers.get(i).add(num_points);
        }
        return centers;
    }
}
