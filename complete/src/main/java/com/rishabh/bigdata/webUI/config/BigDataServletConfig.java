package com.rishabh.bigdata.webUI.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.config.annotation.DefaultServletHandlerConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
 
@Configuration
@EnableWebMvc
@ComponentScan("com.rishabh.bigdata.webUI.controllers")
@Import(com.rishabh.bigdata.webUI.config.ThymeleafConfig.class)


public class BigDataServletConfig extends WebMvcConfigurerAdapter {
 
    public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
        configurer.enable();
    }

	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry.addResourceHandler("/css/**").addResourceLocations("/css/");
		// TODO Auto-generated method stub
		//super.addResourceHandlers(registry);
	}
}
