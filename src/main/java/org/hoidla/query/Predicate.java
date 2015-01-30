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

import java.io.Serializable;

import org.hoidla.window.WindowUtils;

/**
 * Evaluates simple predicate applicable to raw data or some scalar function of the raw data
 * @author pranab
 *
 */
public class Predicate implements Serializable {
	private String operand;
	private String operator;
	private double value;
	private int percentTrue;
	private double turningPointThreshold;
	private boolean relational;
	private Object[] parameters;
	
	public static final String OPERATOR_GT = "gt";
	public static final String OPERATOR_LT = "lt";
	public static final String OPERATOR_GE = "ge";
	public static final String OPERATOR_LE = "le";
	
	public static final String OPERAND_NONE = "none";
	public static final String OPERAND_MEAN = "mean";
	public static final String OPERAND_STD_DEV = "stdDev";
	public static final String OPERAND_MEDIAN = "median";
	public static final String OPERAND_ENTROPY = "entropy";
	public static final String OPERAND_TURNING_POINT = "turningPoint";
	public static final String OPERAND_ABOVE_THRESHOLD = "aboveThreshold";
	public static final String OPERAND_BELOW_THRESHOLD = "belowThreshold";
	public static final String OPERAND_ABOVE_THRESHOLD_WITH_MIN_COUNT = "aboveThresholdWithMinCount";
	public static final String OPERAND_BELOW_THRESHOLD_WITH_MIN_COUNT = "belowThresholdWithMinCount";
	
	
	public Predicate() {
	}
	
	/**
	 * @param operand
	 * @param operator
	 * @param value
	 */
	public Predicate(String operand, String operator, double value) {
		this.operand = operand;
		this.operator = operator;
		this.value = value;
		relational = true;
	}

	/**
	 * @param operand
	 * @return
	 */
	public Predicate withOperand(String operand) {
		this.operand = operand;
		return this;
	}
	
	/**
	 * @param operator
	 * @return
	 */
	public Predicate withOperator(String operator) {
		this.operator = operator;
		relational = true;
		return this;
	}

	/**
	 * @param value
	 * @return
	 */
	public Predicate withValue(double value) {
		this.value = value;
		return this;
	}

	/**
	 * @param parameters
	 * @return
	 */
	public Predicate withParameters(Object... parameters ) {
		this.parameters = parameters;
		return this;
	}
	
	/**
	 * @param operator
	 * @param value
	 */
	public Predicate(String operator, double value) {
		this.operator = operator;
		this.value = value;
	}

	/**
	 * functional predicate
	 * @param data
	 * @return
	 */
	public boolean evaluate(double[] data) {
		boolean result = false;
		if (operand.equals(OPERAND_ABOVE_THRESHOLD)) {
			result = WindowUtils.allValuesAbove(data, (Double)parameters[0]);
		} else if (operand.equals(OPERAND_BELOW_THRESHOLD)) {
			result = WindowUtils.allValuesBelow(data, (Double)parameters[0]);
		} else if (operand.equals(OPERAND_ABOVE_THRESHOLD_WITH_MIN_COUNT)) {
			result = WindowUtils.valuesAbove(data, (Double)parameters[0], (Double)parameters[1]);
		} else if (operand.equals(OPERAND_BELOW_THRESHOLD_WITH_MIN_COUNT)) {
			result = WindowUtils.valuesBelow(data, (Double)parameters[0], (Double)parameters[1]);
		} else {
			throw new IllegalArgumentException("illegal functional predicate name");
		}
		return result;
	}
	
	/**
	 * relational predicate
	 * @param operandValue
	 * @return
	 */
	public boolean evaluate(double operandValue) {
		boolean result = false;
		if (operator.equals(OPERATOR_GT)) {
			result = operandValue > value;
		} else if (operator.equals(OPERATOR_LT)) {
			result = operandValue < value;
		} else if (operator.equals(OPERATOR_GE)) {
			result = operandValue >= value;
		} else if (operator.equals(OPERATOR_LE)) {
			result = operandValue <= value;
		} else {
			throw new IllegalArgumentException("illegal operator");
		}
		return result;
	}

	
	public String getOperand() {
		return operand;
	}

	public void setOperand(String operand) {
		this.operand = operand;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}
	
	public int getPercentTrue() {
		return percentTrue;
	}

	public void setPercentTrue(int percentTrue) {
		this.percentTrue = percentTrue;
	}

	public double getTurningPointThreshold() {
		return turningPointThreshold;
	}

	public void setTurningPointThreshold(double turningPointThreshold) {
		this.turningPointThreshold = turningPointThreshold;
	}

	public boolean isOperandScalar() {
		return !operand.equals(OPERAND_NONE) && !operand.equals(OPERAND_TURNING_POINT);
	}

	public boolean isRelational() {
		return relational;
	}

	public void setRelational(boolean relational) {
		this.relational = relational;
	}

	public Object[] getParameters() {
		return parameters;
	}

	public void setParameters(Object[] parameters) {
		this.parameters = parameters;
	}
}
