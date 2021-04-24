package queryparts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Measuring implements Serializable{

	private List<String> mAttr;
	
	public Measuring(String mPart) {
		this.mAttr = new ArrayList<>();
		this.mAttr.add(mPart);
	}

	public Measuring() {
		this.mAttr = new ArrayList<>();
	}
	
	public void addMeasuring(Measuring m) {
		this.mAttr.add(m.getMeasuringAttribute());
	}
	
	public String getMeasuringAttribute() {
		if(this.mAttr != null) return this.mAttr.get(0);
		else return null;
	}
	
	public List<String> getMeasuringAttributes() {
		return this.mAttr;
	}

	@Override
	public String toString() {
		return "Measuring [mAttr=" + mAttr + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mAttr == null) ? 0 : mAttr.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Measuring other = (Measuring) obj;
		if (mAttr == null) {
			if (other.mAttr != null)
				return false;
		} else if (!mAttr.equals(other.mAttr))
			return false;
		return true;
	}

}
