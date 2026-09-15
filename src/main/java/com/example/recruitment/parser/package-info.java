/**
 * Turns raw uploaded input into domain-ready data: ExcelParser reads
 * candidate rows out of an .xlsx workbook, JobDescriptionParser reads
 * job requirements out of the structured text format. Owns all Apache POI
 * usage and all input validation for these two formats.
 */
package com.example.recruitment.parser;
