package mitll.xdata.dataset.bitcoin.features;

/**
 * Created by go22670 on 5/25/16.
 */
public class MyEdge implements Comparable<MyEdge>{
  public final long source;
  public final long target;

  MyEdge(long source, long target) {
    this.source = source;
    this.target = target;
  }

  public int hashCode() {
    return (int)source + 31*(int)target;
  }

  @Override
  public int compareTo(MyEdge o) {
    int  i = Long.valueOf(source).compareTo(source);
    if (i == 0) i = Long.valueOf(target).compareTo(target);
    return i;
  }

  public boolean equals(Object other) {
    MyEdge oe = (MyEdge) other;
    return compareTo(oe) == 0;
  }
}
