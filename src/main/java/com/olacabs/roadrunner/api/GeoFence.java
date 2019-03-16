//************************************************************
// Copyright 2019 ANI Technologies Pvt. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//************************************************************
package com.olacabs.roadrunner.api;

import java.util.Arrays;

public class GeoFence {
	
	public enum Type {INCLUDE, EXCLUDE};
	
	private Type type;
	
	private double[] boundary;

	/**
	 * @param type
	 * @param boundary minLat, minLon, maxLat, maxLon
	 */
	public GeoFence(Type type, double[] boundary) {
		this.type = type;
		this.boundary = boundary;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public double[] getBoundary() {
		return boundary;
	}

	/**
	 * @param boundary minLat, minLon, maxLat, maxLon
	 */
	public void setBoundary(double[] boundary) {
		this.boundary = boundary;
	}

	@Override
	public String toString() {
		return "GeoFence [type=" + type + ", boundary=" + Arrays.toString(boundary) + "]";
	}
	
}
