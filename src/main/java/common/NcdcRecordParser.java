package common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
	private static final int MISSING_TEMPERATURE = 9999;

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");

	private String stationId;

	private String observationDateString;
	private String year;
	private String airTemperatureString;
	private int airTemperature;
	private boolean airTemperatureMalformed;
	private String quality;

	public void parse(String record) {
		stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
		observationDateString = record.substring(15, 27);
		year = record.substring(15, 19);
		airTemperatureMalformed = false;
		if (record.charAt(87) == '+') {
			airTemperatureString = record.substring(88, 92);
			airTemperature = Integer.parseInt(airTemperatureString);
		} else if (record.charAt(87) == '-') {
			airTemperatureString = record.substring(87, 92);
			airTemperature = Integer.parseInt(airTemperatureString);
		} else {
			airTemperatureMalformed = true;
		}
		airTemperature = Integer.parseInt(airTemperatureString);
		quality = record.substring(92, 93);
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	public boolean isValidTemperature() {
		return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
				&& quality.matches("[01459]");
	}

	public boolean isMalformedTemperature() {
		return airTemperatureMalformed;
	}

	public boolean isMissingTemperature() {
		return airTemperature == MISSING_TEMPERATURE;
	}

	/**
	 * @return the airTemperatureString
	 */
	public String getAirTemperatureString() {
		return airTemperatureString;
	}

	/**
	 * @param airTemperatureString
	 *            the airTemperatureString to set
	 */
	public void setAirTemperatureString(String airTemperatureString) {
		this.airTemperatureString = airTemperatureString;
	}

	/**
	 * @return the airTemperatureMalformed
	 */
	public boolean isAirTemperatureMalformed() {
		return airTemperatureMalformed;
	}

	/**
	 * @param airTemperatureMalformed
	 *            the airTemperatureMalformed to set
	 */
	public void setAirTemperatureMalformed(boolean airTemperatureMalformed) {
		this.airTemperatureMalformed = airTemperatureMalformed;
	}

	/**
	 * @return the stationId
	 */
	public String getStationId() {
		return stationId;
	}

	/**
	 * @param stationId
	 *            the stationId to set
	 */
	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	/**
	 * @return the observationDateString
	 */
	public String getObservationDateString() {
		return observationDateString;
	}

	/**
	 * @param observationDateString
	 *            the observationDateString to set
	 */
	public void setObservationDateString(String observationDateString) {
		this.observationDateString = observationDateString;
	}

	/**
	 * @return the year
	 */
	public String getYear() {
		return year;
	}

	/**
	 * @param year
	 *            the year to set
	 */
	public void setYear(String year) {
		this.year = year;
	}

	/**
	 * @return the airTemperature
	 */
	public int getAirTemperature() {
		return airTemperature;
	}

	/**
	 * @param airTemperature
	 *            the airTemperature to set
	 */
	public void setAirTemperature(int airTemperature) {
		this.airTemperature = airTemperature;
	}

	/**
	 * @return the quality
	 */
	public String getQuality() {
		return quality;
	}

	/**
	 * @param quality
	 *            the quality to set
	 */
	public void setQuality(String quality) {
		this.quality = quality;
	}

}
