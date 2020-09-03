class Geotransform:
    """[summary]
        This method exists because I can never remember what the 5 array elements are

        Note. We don't deal with rotation here

           # Remember:
            # [0]/* top left x */
            # [1]/* w-e pixel resolution */
            # [2]/* rotation, 0 if image is "north up" */
            # [3]/* top left y */
            # [4]/* rotation, 0 if image is "north up" */
            # [5]/* n-s pixel resolution */
    """

    def __init__(self, gt):
        self.gt = list(gt)

    def Left(self):
        return self.gt[0]

    def SetLeft(self, left):
        self.gt[0] = left

    def Top(self):
        return self.gt[3]

    def SetTop(self, top):
        self.gt[3] = top

    def CellWidth(self):
        return self.gt[1]

    def CellHeight(self):
        return self.gt[5]

    def SetCellWidth(self, cellwidth):
        self.gt[1] = cellwidth

    def SetCellHeight(self, cellwidth):
        self.gt[5] = cellwidth
