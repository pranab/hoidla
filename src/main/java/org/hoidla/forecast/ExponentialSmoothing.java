/*
 * hoidla: various algorithms for Big Data solutions
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

package org.hoidla.forecast;

import java.util.ArrayList;
import java.util.List;

import org.chombo.util.BasicUtils;

/**
 * Exponential smoothing forecasting
 * @author pranab
 *
 */
public class ExponentialSmoothing implements Forecaster{
	private String trendType;
	private String seasonalityType;
	private double alpha;
	private double beta;
	private double gama;
	private int seasonPeriod;
	private double[] level = new double[2];
	private double[] trendComp = new double[2];
	private boolean initialized;
	private List<Double> seasComp = new ArrayList<Double>();
	private int indx =01;
	
	/**
	 * @param configParams
	 */
	public ExponentialSmoothing(String[] configParams) {
		int i = 0;
		trendType = configParams[i++];
		seasonalityType = configParams[i++];
		if (trendType.equals("n")) {
			if (seasonalityType.equals("n")) {
				alpha = Double.parseDouble(configParams[i++]);
			}
			else if (seasonalityType.equals("a")) {
				alpha = Double.parseDouble(configParams[i++]);
				gama = Double.parseDouble(configParams[i++]);
				seasonPeriod = Integer.parseInt(configParams[i++]);
			} else {
				BasicUtils.assertFail("invalid seasonality mode");
			}
		} else if (trendType.equals("a")) {
			if (seasonalityType.equals("n")) {
				alpha = Double.parseDouble(configParams[i++]);
				beta = Double.parseDouble(configParams[i++]);
			}
			else if (seasonalityType.equals("a")) {
				alpha = Double.parseDouble(configParams[i++]);
				beta = Double.parseDouble(configParams[i++]);
				gama = Double.parseDouble(configParams[i++]);
				seasonPeriod = Integer.parseInt(configParams[i++]);
			} else {
				BasicUtils.assertFail("invalid seasonality mode");
			}
		} else {
			BasicUtils.assertFail("invalid trend mode");
		}
	}

	/**
	 * @param data
	 * @return
	 */
	public double forecast(List<Double> data) {
		double forecast = 0;
		double current = data.get(1);
		double lev = 0;
		double trend = 0;
		double seas = 0;
		double seasPast = 0;
		int pastSeasonIndex = 0;
		
		if (!initialized) {
			level[0] = data.get(0);
			level[1] = data.get(1);
			trendComp[0] = 0;
			trendComp[1] = data.get(1) - data.get(0);
			initialized = true;
		}
		
		if (trendType.equals("n")) {
			if (seasonalityType.equals("n")) {
				lev = alpha * current + (1.0 - alpha) * level[0];
				forecast =  lev;
				updateLevel(lev);
			}
			else if (seasonalityType.equals("a")) {
				pastSeasonIndex = indx - seasonPeriod; 
				seasPast = getPastSeasonComponent(pastSeasonIndex);
				lev = alpha * (current - seasPast)  + (1.0 - alpha) * level[0];
				seas = gama * (current - level[0]) + (1 - gama) * seasPast;
				seasPast = getPastSeasonComponent(pastSeasonIndex + 1);
				forecast = lev + seasPast;
				updateLevel(lev);
				updateSeasonality(seas);
			} 
		} else if (trendType.equals("a")) {
			if (seasonalityType.equals("n")) {
				lev = alpha * current +  (1.0 - alpha) * (level[0] + trendComp[0]);
				trend = beta * (level[1] - level[0]) + (1 - beta) * trendComp[0];
				forecast = lev + trend;
				updateLevel(lev);
				updateTrend(trend);
			}
			else if (seasonalityType.equals("a")) {
				pastSeasonIndex = indx - seasonPeriod; 
				seasPast = getPastSeasonComponent(pastSeasonIndex);
				lev = alpha * (current - seasPast)  + (1.0 - alpha) * (level[0] + trendComp[0]);	
				trend = beta * (level[1] - level[0]) + (1 - beta) * trendComp[0];
				seas = gama * (current - level[0] - trendComp[0]) + (1 - gama) * seasPast;
				seasPast = getPastSeasonComponent(pastSeasonIndex + 1);
				forecast = lev + trend + seasPast;
				updateLevel(lev);
				updateTrend(trend);
				updateSeasonality(seas);
			} 
		} 
		
		++indx;
		return forecast;
	}
	
	/**
	 * @param sIndx
	 * @return
	 */
	private double getPastSeasonComponent(int sIndx) {
		double seasPast = 0;
		if (sIndx >= 0) {
			seasPast = seasComp.get(sIndx);
		}
		return seasPast;
	}
	
	/**
	 * @param lev
	 */
	private void updateLevel(double lev) {
		level[0] = level[1];
		level[1] = lev;
	}
	
	/**
	 * @param lev
	 */
	private void updateTrend(double trend) {
		trendComp[0] = trendComp[1];
		trendComp[1] = trend;
	}

	/**
	 * @param seas
	 */
	private void updateSeasonality(double seas) {
		seasComp.add(seas);
		if (seasComp.size() > seasonPeriod) {
			seasComp.remove(0);
		}
	}

}
