package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    val rectangle = queryRectangle.split(",")
    val rect_x1 = rectangle(0).toDouble
    val rect_y1 = rectangle(1).toDouble
    val rect_x2 = rectangle(2).toDouble
    val rect_y2 = rectangle(3).toDouble
    val point = pointString.split(",")
    val point_x = point(0).toDouble
    val point_y = point(1).toDouble
    if(rect_x1 < rect_x2 && rect_y1 < rect_y2){
      if(point_x >= rect_x1 && point_y >= rect_y1 && point_x <= rect_x2 && point_y <= rect_y2 ){
        return true
      }
    }
    else if(rect_x1 > rect_x2 && rect_y1 > rect_y2){
      if(point_x >= rect_x2 && point_y >= rect_y2 && point_x <= rect_x1 && point_y <= rect_y1 ){
        return true
      }
    }
    else if(rect_x1 < rect_x2 && rect_y1 > rect_y2){
      if(point_x >= rect_x1 && point_y <= rect_y1 && point_x <= rect_x2 && point_y >= rect_y2 ){
        return true
      }
    }
    else if(rect_x1 > rect_x2 && rect_y1 < rect_y2){
      if(point_x <= rect_x1 && point_y >= rect_y1 && point_x >= rect_x2 && point_y <= rect_y2 ){
        return true
      }
    }
    return false
  }
    // YOU NEED TO CHANGE THIS PART
  

  // YOU NEED TO CHANGE THIS PART

}
