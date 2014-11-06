package com.tuan.elasticsearch.river.json;

import java.util.Map;

public class RiverProduct {

    public Action action;
    public String index;
    public String type;
    public String id;
    public Map<String, Object> product;

    public static enum Action {
        INDEX, DELETE
    }

    public static RiverProduct delete(String index, String type, String id) {
        RiverProduct product = new RiverProduct();
        product.action = Action.DELETE;
        product.index = index;
        product.type = type;
        product.id = id;
        return product;
    }

    public static RiverProduct index(String index, String type, String id, Map<String, Object> product) {
        RiverProduct riverProduct = new RiverProduct();
        riverProduct.action = Action.INDEX;
        riverProduct.index = index;
        riverProduct.type = type;
        riverProduct.id = id;
        riverProduct.product = product;
        return riverProduct;
    }

	@Override
	public String toString() {
		return "RiverProduct [action=" + action + ", index=" + index
				+ ", type=" + type + ", id=" + id + ", product=" + product
				+ "]";
	}

}
