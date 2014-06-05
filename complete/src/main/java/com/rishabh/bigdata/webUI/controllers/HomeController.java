package com.rishabh.bigdata.webUI.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.rishabh.bigdata.hadoop.HadoopFile;
import com.rishabh.bigdata.hadoop.HadoopManager;
import com.rishabh.bigdata.mongo.MongoManager;

@Controller
public class HomeController {

	@RequestMapping(value="/", method=RequestMethod.GET)
    public String index(Model model) {
    	return "index";
    }
	
    @RequestMapping(value="/backupCollection", method=RequestMethod.GET, params="collection")
    public String backupCollection(@RequestParam(value="collection", required=false)String collectionName, Model model) {
    	String mBackupResult = MongoManager.getInstance("localhost", "RipTideEbayData").backupData(collectionName);
    	
    	if (mBackupResult.equals("Error")) {
    		model.addAttribute("info", "There was an error backing up data. Please view event logs for more information.");
    	} else {
    		model.addAttribute("info", collectionName + " has been successfully backed up.");
    	}	
    	
        return "home";
    }
}
