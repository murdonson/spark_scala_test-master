class MySecondSortKey(val first:Int,val second:Int) extends Ordered[MySecondSortKey] with Serializable{
  override def compare(that: MySecondSortKey): Int = {

    if(this.first-that.first==0)
      {
        this.second-that.second
      }
    else{
      this.first-that.first
    }

  }
}
