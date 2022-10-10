from pypelyne import Transform as Super

class Transform(Super):

    def process(self):
        """
        """
        line = self._dataset.first() # le uma linha
        while line:
            if not ''.join(line).strip():
                self._dataset.current_delete()
                line = self._dataset.current()
                continue
            try:
                line = self._dataset.next()
            except:
                break
