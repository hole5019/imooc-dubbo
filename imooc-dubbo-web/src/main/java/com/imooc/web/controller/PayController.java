package com.imooc.web.controller;

import com.imooc.curator.utils.ZKCurator;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.imooc.common.utils.IMoocJSONResult;
import com.imooc.web.service.CulsterService;

/**
 * @Description: 订购商品controller
 */
@Controller
public class PayController {
	
	@Autowired
	private CulsterService buyService;

	@Autowired
	private ZKCurator zkCurator;
	
	@RequestMapping("/index")
	public String index() {
		return "index";
	}
	
	@GetMapping("/buy")
	@ResponseBody
	public IMoocJSONResult doGetlogin(String itemId) {
		
		if (StringUtils.isNotBlank(itemId)) {
			buyService.displayBuy(itemId);
		} else {
			return IMoocJSONResult.errorMsg("商品id不能为空");
		}
		
		return IMoocJSONResult.ok();
	}

	@GetMapping("/buy2")
	@ResponseBody
	public IMoocJSONResult doGetlogin2(String itemId) {

		if (StringUtils.isNotBlank(itemId)) {
			buyService.displayBuy(itemId);
		} else {
			return IMoocJSONResult.errorMsg("商品id不能为空");
		}

		return IMoocJSONResult.ok();
	}

	/**
	 * 判断zk是否连接
	 * @return
	 */
	@RequestMapping("/isZKAlive")
	@ResponseBody
	public IMoocJSONResult isZKAlive(){
		boolean isAlive = zkCurator.isZKAlive();
		String result = isAlive ? "连接" : "断开";
		return IMoocJSONResult.ok(result);
	}
	
}
