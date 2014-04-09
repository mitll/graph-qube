package mitll.xdata.binding;

import java.text.SimpleDateFormat;
import java.util.Date;

import mitll.xdata.binding.Binding.Edge;

public class TestEdge implements Edge<String> {
	private String source;
	private String target;
	private long time;
	private double amount;

	/** deviation from population mean usd */
	private double deviationFromPopulation;

	/** deviation from target's mean usd */
	private double deviationFromOwnCredits;

	/** deviation from source's mean usd */
	private double deviationFromOwnDebits;

	public TestEdge(String source, String target, long time, double amount, double deviationFromPopulation,
			double deviationFromOwnCredits, double deviationFromOwnDebits) {
		this.source = source;
		this.target = target;
		this.time = time;
		this.amount = amount;
		this.deviationFromPopulation = deviationFromPopulation;
		this.deviationFromOwnCredits = deviationFromOwnCredits;
		this.deviationFromOwnDebits = deviationFromOwnDebits;
	}

	@Override
	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	@Override
	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	@Override
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	public double getDeviationFromPopulation() {
		return deviationFromPopulation;
	}

	public void setDeviationFromPopulation(double deviationFromPopulation) {
		this.deviationFromPopulation = deviationFromPopulation;
	}

	public double getDeviationFromOwnCredits() {
		return deviationFromOwnCredits;
	}

	public void setDeviationFromOwnCredits(double deviationFromOwnCredits) {
		this.deviationFromOwnCredits = deviationFromOwnCredits;
	}

	public double getDeviationFromOwnDebits() {
		return deviationFromOwnDebits;
	}

	public void setDeviationFromOwnDebits(double deviationFromOwnDebits) {
		this.deviationFromOwnDebits = deviationFromOwnDebits;
	}

	public String toString() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		String s = "";
		s += source;
		s += "\t" + target;
		s += "\t" + time;
		s += "\t" + sdf.format(new Date(time));
		s += "\t" + amount;
		s += "\t" + deviationFromPopulation;
		s += "\t" + deviationFromOwnCredits;
		s += "\t" + deviationFromOwnDebits;
		return s;
	}

	@Override
	public int compareTo(Edge o) {
		long t1 = this.time;
		long t2 = o.getTime();
		if (t1 < t2) {
			return -1;
		} else if (t1 > t2) {
			return 1;
		} else {
			return 0;
		}
	}
}
