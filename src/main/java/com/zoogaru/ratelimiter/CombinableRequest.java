package com.zoogaru.ratelimiter;

public interface CombinableRequest extends Request {
    boolean isCombinable() ;
}
