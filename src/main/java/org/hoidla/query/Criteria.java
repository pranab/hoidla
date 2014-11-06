/*
 * hoidla: various streaming algorithms for Big Data solutions
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hoidla.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hoidla.window.SizeBoundWindow;
import org.hoidla.window.WindowUtils;

/**
 * Evaluates a list conjunctive predicates 
 * @author pranab
 *
 */
public class Criteria {
	private List<Predicate> predicates;
	private double[] data;
	private int[] intData;
	private Map<String, Double> operandValues = new HashMap<String, Double>();
	
	/**
	 * @return
	 */
	public List<Predicate> getPredicates() {
		return predicates;
	}

	/**
	 * @param predicates
	 */
	public void setPredicates(List<Predicate> predicates) {
		this.predicates = predicates;
	}
	
	/**
	 * @param window
	 * @return
	 */
	public<T> boolean evaluate(SizeBoundWindow<T> window) {
		boolean result = true;
		data = WindowUtils.getDoubleArray(window);
		intData = WindowUtils.getIntArray(window);
		for (Predicate pred : predicates) {
			String operand = pred.getOperand();
			double opValue = getOperandValue(operand);
			result = result && pred.evaluate(opValue);
			if (!result) {
				break;
			}
		}
		return result;
	}
	
	/**
	 * @param operand
	 * @return
	 */
	private double getOperandValue(String operand) {
		Double opValue = operandValues.get(operand);
		if (null == opValue) {
			if (operand.equals(Predicate.OPERAND_MEAN)) {
				opValue = WindowUtils.getMean(data);
			} else if (operand.equals(Predicate.OPERAND_STD_DEV)) {
				opValue = WindowUtils.getStdDev(data);
			} else if (operand.equals(Predicate.OPERAND_MEDIAN)) {
				opValue = WindowUtils.getMean(data);
			} else if (operand.equals(Predicate.OPERAND_ENTROPY)) {
				opValue = WindowUtils.getEntropy(intData);
			}
		}
		
		return opValue;
	}
}
