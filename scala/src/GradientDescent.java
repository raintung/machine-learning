import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GradientDescent {
	static class DataItem{
		DataItem(float x,float y){
			this.x =x;
			this.y = y;
		}
		float x;
		float y;
	}
	
	public static void normalization(List<DataItem> items) {
	    float min = 100000;
	    float max = 0;
	    for(DataItem item : items) {
	        min = Math.min(min, item.x);
	        max = Math.max(max, item.x);
	    }
	    float delta = max - min;
	    for(DataItem item : items) {
	        item.x = (item.x - min) / delta;
	    }
	}
	public static void main(String[] args){
		List<DataItem> items = new ArrayList<DataItem>();
		items.add(new DataItem(34.9f,534.0f));
		items.add(new DataItem(24.9f,334.0f));
		items.add(new DataItem(12.9f,134.0f));
		normalization(items);
		int repetion = 1500;
	    float learningRate = 0.1f;
	    float[] theta = new float[2];
	    Arrays.fill(theta, 0);
	    float[] hmatrix = new float[items.size()];
	    Arrays.fill(hmatrix, 0);
	    int k=0;
	    float s1 = 1.0f / items.size();
	    float sum1=0, sum2=0;
	    for(int i=0; i<repetion; i++) {
	        for(k=0; k<items.size(); k++ ) {
	            hmatrix[k] = ((theta[0] + theta[1]*items.get(k).x) - items.get(k).y);
	        }
//sum theta0 + theta1*x1 -y1
	        // sum (theta0 + theta1*xi -yi)xi
	        for(k=0; k<items.size(); k++ ) {
	            sum1 += hmatrix[k];
	            sum2 += hmatrix[k]*items.get(k).x;
	        }

	        sum1 = learningRate*s1*sum1;
	        sum2 = learningRate*s1*sum2;

	        // 更新 参数theta
	        theta[0] = theta[0] - sum1;
	        theta[1] = theta[1] - sum2;
	    }
	    for(float v :theta){
	    	System.out.println(v);
	    }
	    float result = theta[0] + theta[1]*20;
	    System.out.println(result);
	    //return theta;
	}

}
