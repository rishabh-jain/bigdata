package com.rishabh.bigdata.webUI.controllers;

import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.rishabh.bigdata.log.Logger;
import com.rishabh.bigdata.mongo.JSONImportManager;

@Controller
public class JSONController {
	@RequestMapping(value = "/json/import", method = RequestMethod.GET)
	public String json_import(Model model) {
		return "json/import/index";
	}
	
	@RequestMapping(value="/json/import/status", method=RequestMethod.POST, params={"pFilePath", "pMongoCollection"})
    public String logs_view(@RequestParam(value="pFilePath", required=true)String mFilePath,@RequestParam(value="pMongoCollection", required=true)String mMongoCollection, Model model) {
    	JSONImportManager.getInstance().importDataFromFile(mFilePath, mMongoCollection);
    	return "json/import/index";
    }
}
