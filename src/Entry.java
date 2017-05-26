public class Entry implements Comparable<Entry>{
	public int theKey;
	public String theVal;
	
	public Entry(int freq, String word) {
		this.theKey = freq;
		this.theVal = word;
	}
	
	public int compareTo(Entry entry) {
		if (this.theKey == entry.theKey) {
			return this.theVal.compareTo(entry.theVal);
		}
		return this.theKey - entry.theKey;
	}
}
