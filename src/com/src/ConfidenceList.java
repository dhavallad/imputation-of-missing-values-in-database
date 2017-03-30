/**
 * 
 */
package com.src;

import java.util.List;

/**
 * @author dhavallad
 *
 */
public class ConfidenceList {
		private List powerSet;
		private int confidence;
		private List<tableValues> tb;
		private List<tableValues> tb1;
		public List<tableValues> getTb1() {
			return tb1;
		}
		public void setTb1(List<tableValues> tb1) {
			this.tb1 = tb1;
		}
		int rowID;
		public int getRowID() {
			return rowID;
		}
		public void setRowID(int rowID) {
			this.rowID = rowID;
		}
		public List getPowerSet() {
			return powerSet;
		}
		public void setPowerSet(List powerSet) {
			this.powerSet = powerSet;
		}
		public int getConfidence() {
			return confidence;
		}
		public void setConfidence(int confidence) {
			this.confidence = confidence;
		}
		public List<tableValues> getTb() {
			return tb;
		}
		public void setTb(List<tableValues> tb) {
			this.tb = tb;
		}
}
